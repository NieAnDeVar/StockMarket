using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using StockAggregator.Connectors;
using StockAggregator.Contracts;
using StockAggregator.Options;
using StockAggregator.Tests.Support;

namespace StockAggregator.Tests;

// Real sockets against scripted in-process exchanges, no mocks.
public sealed class ConnectorIntegrationTests
{
    private static string AlphaTick(long seq) =>
        $"{{\"symbol\":\"AAPL\",\"price\":187.3,\"qty\":100,\"ts\":\"2026-01-05T12:30:00Z\",\"seq\":{seq}}}";

    private static AggregatorOptions Options(int idleSec = 15) =>
        new() { ConnectTimeoutSec = 5, IdleTimeoutSec = idleSec };

    private static ExchangeConnectorWorker StartConnector(
        string id, string wsUrl, ChannelWriter<IncomingTick> writer, AggregatorOptions options)
    {
        var source = new SourceOptions { Id = id, Url = wsUrl, Format = "Alpha" };
        var worker = new ExchangeConnectorWorker(source, writer, options,
            new SourceStateTracker(), NullLogger<ExchangeConnectorWorker>.Instance);
        worker.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return worker;
    }

    [Fact]
    public async Task ServerClosesRepeatedly_ConnectorKeepsReconnecting()
    {
        await using var server = await TestWsServer.StartAsync(
            (socket, ct) => socket.CloseAsync(
                WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None));

        var channel = Channel.CreateUnbounded<IncomingTick>();
        var worker = StartConnector("flaky", server.WsUrl + "/ws", channel.Writer, Options());
        try
        {
            var reconnected = await AsyncWait.UntilAsync(
                () => server.ConnectionCount >= 3, TimeSpan.FromSeconds(15));
            Assert.True(reconnected, $"expected >= 3 reconnects, got {server.ConnectionCount}");
        }
        finally { await worker.StopAsync(CancellationToken.None); }
    }

    // "Breaking" scenario #2 from the assignment: one source dies and reconnects
    // in a loop while the other must keep streaming undisturbed.
    [Fact]
    public async Task FlakySourceDoesNotStarveStableSource()
    {
        await using var stable = await TestWsServer.StartAsync(async (socket, ct) =>
        {
            var seq = 0L;
            while (!ct.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                await TestWsServer.SendTextAsync(socket, AlphaTick(seq++), ct: ct);
                await Task.Delay(20, ct);
            }
        });
        await using var flaky = await TestWsServer.StartAsync(
            (socket, ct) => socket.CloseAsync(
                WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None));

        var channel = Channel.CreateUnbounded<IncomingTick>();
        var received = new ConcurrentQueue<(string Source, DateTimeOffset At)>();
        var collector = Task.Run(async () =>
        {
            await foreach (var t in channel.Reader.ReadAllAsync())
                received.Enqueue((t.SourceId, t.ReceivedAtUtc));
        });

        var stableWorker = StartConnector("stable", stable.WsUrl + "/ws", channel.Writer, Options());
        var flakyWorker = StartConnector("flaky", flaky.WsUrl + "/ws", channel.Writer, Options());
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(4));

            var stableTicks = received.Where(r => r.Source == "stable").OrderBy(r => r.At).ToList();
            Assert.True(stableTicks.Count > 100, $"stable stream too thin: {stableTicks.Count} ticks");
            Assert.True(flaky.ConnectionCount >= 2, "flaky source never reconnected");

            // no gap in the stable stream long enough to blame the flaky neighbor
            var maxGap = stableTicks.Zip(stableTicks.Skip(1), (a, b) => b.At - a.At).Max();
            Assert.True(maxGap < TimeSpan.FromSeconds(1.5), $"stable stream stalled for {maxGap}");
        }
        finally
        {
            await Task.WhenAll(
                stableWorker.StopAsync(CancellationToken.None),
                flakyWorker.StopAsync(CancellationToken.None));
            channel.Writer.Complete();
            await collector;
        }
    }

    [Fact]
    public async Task SilentConnection_IsKilledByIdleTimeout()
    {
        // accepts and says nothing, the half-open case TCP itself never reports
        await using var server = await TestWsServer.StartAsync(
            (socket, ct) => Task.Delay(Timeout.Infinite, ct));

        var channel = Channel.CreateUnbounded<IncomingTick>();
        var worker = StartConnector("silent", server.WsUrl + "/ws", channel.Writer, Options(idleSec: 1));
        try
        {
            var detected = await AsyncWait.UntilAsync(
                () => server.ConnectionCount >= 2, TimeSpan.FromSeconds(10));
            Assert.True(detected, "idle connection was not detected and reconnected");
            Assert.False(channel.Reader.TryRead(out _)); // silence means silence, no phantom ticks
        }
        finally { await worker.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task FragmentedMessage_IsReassembled()
    {
        var full = AlphaTick(42);
        var third = full.Length / 3;

        await using var server = await TestWsServer.StartAsync(async (socket, ct) =>
        {
            // one logical message in three WS frames
            await TestWsServer.SendTextAsync(socket, full[..third], endOfMessage: false, ct);
            await TestWsServer.SendTextAsync(socket, full[third..(2 * third)], endOfMessage: false, ct);
            await TestWsServer.SendTextAsync(socket, full[(2 * third)..], endOfMessage: true, ct);
            await Task.Delay(TimeSpan.FromSeconds(3), ct); // keep the connection open
        });

        var channel = Channel.CreateUnbounded<IncomingTick>();
        var worker = StartConnector("frag", server.WsUrl + "/ws", channel.Writer, Options());
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var tick = await channel.Reader.ReadAsync(timeout.Token);

            Assert.Equal(full, tick.Raw);
        }
        finally { await worker.StopAsync(CancellationToken.None); }
    }
}
