using System.Threading.Channels;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;

namespace StockAggregator.Storage;

public sealed class TickBatchWriter(
    ChannelReader<NormalizedTick> reader,
    ITickRepository repository,
    DatabaseInitializer dbReady,
    AggregatorOptions options,
    ILogger<TickBatchWriter> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await dbReady.Ready.WaitAsync(stoppingToken);

        var batch = new List<NormalizedTick>(options.BatchSize);

        try
        {
            while (true)
            {
                // The time window starts with the FIRST item of a batch and is not
                // refreshed by subsequent reads — otherwise a slow but steady stream
                // would postpone the flush forever.
                using var window = new CancellationTokenSource(options.BatchMaxDelayMs);

                try
                {
                    batch.Add(await reader.ReadAsync(window.Token));
                    if (batch.Count >= options.BatchSize) // size trigger
                        await FlushAsync(batch);
                }
                catch (OperationCanceledException) // window expired — time trigger
                {
                    if (batch.Count > 0)
                        await FlushAsync(batch);
                }
            }
        }
        catch (ChannelClosedException) { }

        // Drain: channel completed by the stage above, everything left must reach the DB.
        while (reader.TryRead(out var rest))
            batch.Add(rest);

        if (batch.Count > 0)
            await FlushAsync(batch);

        logger.LogInformation("batch writer stopped, drain complete");
    }

    private async Task FlushAsync(List<NormalizedTick> batch)
    {
        try
        {
            var inserted = await repository.SaveBatchAsync(batch, CancellationToken.None);
            AggregatorMetrics.TicksWritten.Inc(inserted);
            // inserted < count => PK conflicts skipped by the DB safety net
            AggregatorMetrics.DbDuplicatesSkipped.Inc(batch.Count - inserted);
        }
        catch (Exception ex)
        {
            // Never silent: every lost tick is counted and logged.
            AggregatorMetrics.TicksDropped.Inc(batch.Count);
            logger.LogError(ex, "batch of {Count} ticks dropped after retries", batch.Count);
        }
        finally
        {
            batch.Clear();
        }
    }
}
