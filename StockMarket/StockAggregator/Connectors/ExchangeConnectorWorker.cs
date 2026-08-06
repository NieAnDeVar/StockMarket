using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;

namespace StockAggregator.Connectors;

/// <summary>
/// One worker per source: connect → stream → on any failure reconnect with backoff.
/// Sources are fully isolated: this worker knows nothing about the others.
/// </summary>
public sealed class ExchangeConnectorWorker(
    SourceOptions source,
    ChannelWriter<IncomingTick> writer,
    AggregatorOptions options,
    ILogger<ExchangeConnectorWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var attempt = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            AggregatorMetrics.SourceUp.WithLabels(source.Id).Set(0);
            try
            {
                await ConnectAndStreamAsync(stoppingToken);
                attempt = 0; // server closed politely — reconnect without penalty
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken); // but not instantly
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                attempt++;
                AggregatorMetrics.Reconnects.WithLabels(source.Id).Inc();
                var delay = Backoff(attempt);
                logger.LogWarning(ex,
                    "source {Source} lost, reconnect #{Attempt} in {DelayMs}ms",
                    source.Id, attempt, (int)delay.TotalMilliseconds);

                try { await Task.Delay(delay, stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }

        AggregatorMetrics.SourceUp.WithLabels(source.Id).Set(0);
        logger.LogInformation("source {Source} connector stopped", source.Id);
    }

    // Full jitter: random(0, min(30s, 1s * 2^n)) — reconnects of several sources
    // don't pile up on a restarting server at the same moments.
    private static TimeSpan Backoff(int attempt)
    {
        var cap = Math.Min(30_000, 1000 * Math.Pow(2, attempt - 1));
        return TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * cap);
    }

    private async Task ConnectAndStreamAsync(CancellationToken ct)
    {
        using var socket = new ClientWebSocket();

        using (var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            connectTimeout.CancelAfter(TimeSpan.FromSeconds(options.ConnectTimeoutSec));
            await socket.ConnectAsync(new Uri(source.Url), connectTimeout.Token);
        }

        logger.LogInformation("source {Source} connected", source.Id);

        AggregatorMetrics.SourceUp.WithLabels(source.Id).Set(1);

        var buffer = new byte[8 * 1024];

        while (socket.State == WebSocketState.Open)
        {
            // Idle detection: silence on the wire == dead connection.
            // A half-open TCP reports nothing, so waiting forever is not an option.
            using var idle = CancellationTokenSource.CreateLinkedTokenSource(ct);
            idle.CancelAfter(TimeSpan.FromSeconds(options.IdleTimeoutSec));

            string? raw;
            try
            {
                raw = await ReceiveMessageAsync(socket, buffer, idle.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                AggregatorMetrics.SourceUp.WithLabels(source.Id).Set(0);
                AggregatorMetrics.Reconnects.WithLabels(source.Id).Inc();
                throw new WebSocketException($"idle timeout: no data for {options.IdleTimeoutSec}s");
            }

            if (raw is null) return; // server sent Close frame — reconnect

            await writer.WriteAsync(
                new IncomingTick(source.Id, raw, DateTimeOffset.UtcNow), ct);
        }
    }

    // A WS message may arrive fragmented — read until EndOfMessage.
    private static async Task<string?> ReceiveMessageAsync(
        ClientWebSocket socket, byte[] buffer, CancellationToken ct)
    {
        using var ms = new MemoryStream();

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, ct);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
                return null;
            }

            ms.Write(buffer, 0, result.Count);
            if (result.EndOfMessage)
                return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
        }
    }
}
