using System.Threading.Channels;
using StockAggregator.Contracts;
using StockAggregator.Processing;

namespace StockAggregator.Observability;

public sealed class MetricsWorker(
    Channel<IncomingTick> rawChannel,
    Channel<NormalizedTick> normalizedChannel,
    Deduplicator dedup) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        var rawMax = 0;
        var normalizedMax = 0;
        var samples = 0;

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            rawMax = Math.Max(rawMax, rawChannel.Reader.Count);
            normalizedMax = Math.Max(normalizedMax, normalizedChannel.Reader.Count);

            if (++samples < 20) continue;
            samples = 0;
            AggregatorMetrics.ChannelOccupancy.WithLabels("raw").Set(rawMax);
            AggregatorMetrics.ChannelOccupancy.WithLabels("normalized").Set(normalizedMax);
            AggregatorMetrics.DedupCacheSize.Set(dedup.Count);
            rawMax = 0;
            normalizedMax = 0;
        }
    }
}
