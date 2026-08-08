using System.Net.WebSockets;
using StockMarketImitator.Streaming;

namespace StockMarketImitator.Tests;

public sealed class ClientSessionTests
{
    // Only Enqueue is exercised, so the socket is never touched.
    private sealed class StubWebSocket : WebSocket
    {
        public override WebSocketState State => WebSocketState.Open;
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override string? SubProtocol => null;
        public override void Abort() { }
        public override void Dispose() { }
        public override Task CloseAsync(WebSocketCloseStatus s, string? d, CancellationToken ct) =>
            throw new NotSupportedException();
        public override Task CloseOutputAsync(WebSocketCloseStatus s, string? d, CancellationToken ct) =>
            throw new NotSupportedException();
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> b, CancellationToken ct) =>
            throw new NotSupportedException();
        public override Task SendAsync(ArraySegment<byte> b, WebSocketMessageType t, bool eom, CancellationToken ct) =>
            throw new NotSupportedException();
    }

    [Fact]
    public void Enqueue_OverCapacity_DropsAndCounts()
    {
        // outbox capacity is 1000 with no reader draining it: the 1001st tick
        // must be dropped and the loss must be counted, not swallowed by DropOldest
        var session = new ClientSession(new StubWebSocket(), TimeSpan.FromMinutes(1));

        for (var i = 0; i < 1001; i++) session.Enqueue("tick");

        Assert.Equal(1, session.DroppedForSlowClient);
    }
}
