using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;

namespace StockAggregator.Connectors;

/// <summary>
/// One worker per source. Fully isolated: failure of one does not affect others.
/// </summary>
public sealed class ExchangeConnectorWorker(
    SourceOptions source,
    ChannelWriter<IncomingTick> writer,
    AggregatorOptions options,
    SourceStateTracker states,
    ILogger<ExchangeConnectorWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var attempt = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            AggregatorMetrics.SourceUp.WithLabels(source.Id).Set(0);
            states.Set(source.Id, up: false);
            try
            {
                await ConnectAndStreamAsync(stoppingToken);
                attempt = 0;
                AggregatorMetrics.Reconnects.WithLabels(source.Id).Inc();
                // short pause after clean close to avoid tight reconnect loop
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
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
        states.Set(source.Id, up: false);
        logger.LogInformation("source {Source} connector stopped", source.Id);
    }

    // full jitter, cap 30s
    private static TimeSpan Backoff(int attempt)
    {
        var exp = Math.Min(attempt - 1, 15);
        var cap = Math.Min(30_000, 1000L << exp);
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
        states.Set(source.Id, up: true);

        var buffer = new byte[8 * 1024];

        while (socket.State == WebSocketState.Open)
        {
            // half-open TCP stays silent; idle timeout is the only reliable detection
            using var idle = CancellationTokenSource.CreateLinkedTokenSource(ct);
            idle.CancelAfter(TimeSpan.FromSeconds(options.IdleTimeoutSec));

            string? raw;
            try
            {
                raw = await ReceiveMessageAsync(socket, buffer, idle.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                throw new WebSocketException($"idle timeout: no data for {options.IdleTimeoutSec}s");
            }

            if (raw is null) return; // Close frame

            await writer.WriteAsync(
                new IncomingTick(source.Id, raw, DateTimeOffset.UtcNow), ct);
        }
    }

    // reassemble fragmented WS messages
    private static async Task<string?> ReceiveMessageAsync(
        ClientWebSocket socket, byte[] buffer, CancellationToken ct)
    {
        using var ms = new MemoryStream(buffer.Length);

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
