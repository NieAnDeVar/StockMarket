using System.Net.WebSockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace StockAggregator.Tests.Support;

// In-process WS "exchange" on a random free port. The handler scripts exactly
// what the exchange does: close instantly, go silent, fragment, duplicate.
// The connector is tested against real sockets, not mocks.
public sealed class TestWsServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    private int _connectionCount;

    private TestWsServer(WebApplication app) => _app = app;

    public string Url { get; private set; } = "";
    public string WsUrl => Url.Replace("http://", "ws://");
    public int ConnectionCount => _connectionCount;

    public static async Task<TestWsServer> StartAsync(
        Func<WebSocket, CancellationToken, Task> handler)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0"); // port 0 = any free port
        builder.Logging.ClearProviders();

        var app = builder.Build();
        var server = new TestWsServer(app);

        app.UseWebSockets();
        app.Map("/ws", async ctx =>
        {
            if (!ctx.WebSockets.IsWebSocketRequest)
            {
                ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            Interlocked.Increment(ref server._connectionCount);
            using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
            await handler(socket, ctx.RequestAborted);
        });

        await app.StartAsync();

        server.Url = app.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses.Single();
        return server;
    }

    public static Task SendTextAsync(WebSocket socket, string text,
        bool endOfMessage = true, CancellationToken ct = default) =>
        socket.SendAsync(Encoding.UTF8.GetBytes(text),
            WebSocketMessageType.Text, endOfMessage, ct);

    public async ValueTask DisposeAsync() => await _app.DisposeAsync();
}

internal static class AsyncWait
{
    public static async Task<bool> UntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(25);
        }
        return condition();
    }
}
