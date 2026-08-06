namespace StockAggregator.Processing;

public sealed class DedupCleanupWorker(
    Deduplicator dedup,
    ILogger<DedupCleanupWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var removed = dedup.EvictOlderThan(DateTimeOffset.UtcNow - dedup.Window);
            if (removed > 0)
                logger.LogDebug("dedup window cleanup: removed {Removed}, size={Size}",
                    removed, dedup.Count);
        }
    }
}
