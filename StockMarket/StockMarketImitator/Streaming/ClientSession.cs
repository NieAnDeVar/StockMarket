using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace StockMarketImitator.Streaming;

/// <summary>
/// One connected client: own bounded channel and a single send loop.
/// Ticks and heartbeats go through RunAsync only — WebSocket forbids
/// concurrent SendAsync, and we satisfy that by construction, without locks.
/// </summary>
public sealed class ClientSession : IAsyncDisposable
{
    private readonly WebSocket _socket;
    private readonly Channel<string> _outbox;
    private readonly TimeSpan _heartbeatInterval;
    private readonly CancellationTokenSource _cts = new();

    private long _droppedForSlowClient;
    public long DroppedForSlowClient => Interlocked.Read(ref _droppedForSlowClient);

    public ClientSession(WebSocket socket, TimeSpan heartbeatInterval)
    {
        _socket = socket;
        _heartbeatInterval = heartbeatInterval;

        // A slow client must not stall others: on overflow the oldest ticks are
        // dropped (like a real exchange does) and the loss is counted.
        _outbox = Channel.CreateBounded<string>(new BoundedChannelOptions(capacity: 1000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public Guid Id { get; } = Guid.NewGuid();

    public void Enqueue(string message)
    {
        if (!_outbox.Writer.TryWrite(message))
        {
            Interlocked.Increment(ref _droppedForSlowClient);
            SimulatorMetrics.DroppedForSlowClients.Inc();
        }
    }

    public void Complete() => _outbox.Writer.TryComplete();

    public void Abort()
    {
        _cts.Cancel();
        _socket.Abort();
    }

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _cts.Token);
        var ct = linked.Token;

        try
        {
            while (true)
            {
                string message;
                try
                {
                    using var hb = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    hb.CancelAfter(_heartbeatInterval);

                    try
                    {
                        message = await _outbox.Reader.ReadAsync(hb.Token);
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        // Application-level heartbeat: System.Net.WebSockets has no API for protocol ping frames
                        // The aggregator recognizes it by "type" and resets its idle timer, skipping normalization
                        message = $$"""{"type":"heartbeat","ts":"{{DateTimeOffset.UtcNow:O}}"}""";
                    }
                }
                catch (ChannelClosedException) { break; }
                catch (OperationCanceledException) { break; }

                await _socket.SendAsync(
                    Encoding.UTF8.GetBytes(message),
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    ct);
            }
        }
        catch (WebSocketException) { }
        catch (OperationCanceledException) { }
        finally
        {
            if (_socket.State == WebSocketState.Open)
            {
                try
                {
                    await _socket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "server shutdown",
                        CancellationToken.None);
                }
                catch
                {
                    // Client may already be gone, closing must never throw
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _socket.Dispose();
        _cts.Dispose();
    }
}
