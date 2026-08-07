using System.Net.WebSockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using StockAggregator.Connectors;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;
using StockAggregator.Processing;
using StockAggregator.Tests.Support;

namespace StockAggregator.Tests;

// End-to-end without the DB: connector -> processing -> dedup, against an
// exchange that duplicates every tick and drops the connection every 100 ticks.
// Invariant under chaos: every seq lands exactly once, nothing lost, nothing twice.
public sealed class PipelineChaosTests
{
    private const int TotalTicks = 300;

    private static string AlphaTick(long seq) =>
        $"{{\"symbol\":\"AAPL\",\"price\":187.3,\"qty\":100,\"ts\":\"2026-01-05T12:30:00Z\",\"seq\":{seq}}}";

    [Fact]
    public async Task DuplicatesPlusDisconnects_EveryTickExactlyOnce()
    {
        var seq = 0;
        await using var server = await TestWsServer.StartAsync(async (socket, ct) =>
        {
            var sentOnThisConnection = 0;
            while (seq < TotalTicks && socket.State == WebSocketState.Open)
            {
                var message = AlphaTick(seq);
                await TestWsServer.SendTextAsync(socket, message, ct: ct);
                await TestWsServer.SendTextAsync(socket, message, ct: ct); // retransmission, same seq
                seq++;
                await Task.Delay(2, ct);

                if (++sentOnThisConnection % 100 == 0)
                    socket.Abort(); // no polite close, just vanish
            }
            // all sent: keep the wire open but silent
            try { await Task.Delay(Timeout.Infinite, ct); } catch (OperationCanceledException) { }
        });

        var raw = Channel.CreateUnbounded<IncomingTick>();
        var normalized = Channel.CreateUnbounded<NormalizedTick>();
        var options = new AggregatorOptions
        {
            ConnectTimeoutSec = 5,
            IdleTimeoutSec = 30, // server goes silent at the end on purpose
            Sources = [new SourceOptions { Id = "chaos", Url = server.WsUrl + "/ws", Format = "Alpha" }]
        };

        var connector = new ExchangeConnectorWorker(options.Sources[0], raw.Writer, options,
            NullLogger<ExchangeConnectorWorker>.Instance);
        var processing = new ProcessingWorker(raw.Reader, normalized.Writer,
            new Deduplicator(TimeSpan.FromSeconds(60)), options,
            [new AlphaNormalizer()]);

        await connector.StartAsync(CancellationToken.None);
        await processing.StartAsync(CancellationToken.None);
        try
        {
            var dedupedBefore = AggregatorMetrics.TicksDeduplicated.Value;
            var parseErrorsBefore = AggregatorMetrics.ParseErrors.WithLabels("chaos").Value;

            var seen = new HashSet<long>();
            var duplicatesThatSlipped = 0;
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(25));

            while (seen.Count < TotalTicks)
            {
                var tick = await normalized.Reader.ReadAsync(timeout.Token);
                if (!seen.Add(tick.SourceSeq))
                    duplicatesThatSlipped++;
            }

            Assert.Equal(0, duplicatesThatSlipped);
            Assert.Equal(TotalTicks, seen.Count);
            Assert.Equal(Enumerable.Range(0, TotalTicks).Select(i => (long)i), seen.OrderBy(x => x));

            // every one of the 300 retransmissions was caught by the in-memory dedup
            Assert.True(AggregatorMetrics.TicksDeduplicated.Value - dedupedBefore >= TotalTicks);
            Assert.Equal(parseErrorsBefore, AggregatorMetrics.ParseErrors.WithLabels("chaos").Value);
        }
        finally
        {
            // same discipline as production: head stops first, then the channel
            // completes, then the stage below drains and ends
            await connector.StopAsync(CancellationToken.None);
            raw.Writer.Complete();
            await processing.StopAsync(CancellationToken.None);
        }
    }
}
