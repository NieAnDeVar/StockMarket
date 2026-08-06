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
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            AggregatorMetrics.ChannelOccupancy.WithLabels("raw").Set(rawChannel.Reader.Count);
            AggregatorMetrics.ChannelOccupancy.WithLabels("normalized").Set(normalizedChannel.Reader.Count);
            AggregatorMetrics.DedupCacheSize.Set(dedup.Count);
        }
    }
}
