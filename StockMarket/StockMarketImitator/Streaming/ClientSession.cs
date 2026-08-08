using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace StockMarketImitator.Streaming;

/// <summary>
/// One connected client: own bounded channel, single send loop
/// (WebSocket forbids concurrent SendAsync, one loop means no locks).
/// </summary>
public sealed class ClientSession : IAsyncDisposable
{
    private const int OutboxCapacity = 1000;

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

        // A slow client must not stall others: overflow drops the oldest ticks, the loss is counted.
        _outbox = Channel.CreateBounded<string>(new BoundedChannelOptions(OutboxCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public Guid Id { get; } = Guid.NewGuid();

    public void Enqueue(string message)
    {
        if (_outbox.Reader.Count >= OutboxCapacity)
        {
            Interlocked.Increment(ref _droppedForSlowClient);
            SimulatorMetrics.DroppedForSlowClients.Inc();
        }
        _outbox.Writer.TryWrite(message);
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
                        // application-level heartbeat: System.Net.WebSockets has no protocol ping API
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
