using System.Threading.Channels;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;

namespace StockAggregator.Storage;

public sealed class TickBatchWriter(
    ChannelReader<NormalizedTick> reader,
    ITickRepository repository,
    IDatabaseReadiness dbReady,
    DbWriteTracker writeTracker,
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
                // The window starts with the FIRST item and is not refreshed,
                // otherwise a slow but steady stream would postpone the flush forever.
                batch.Add(await reader.ReadAsync(CancellationToken.None));
                using var window = new CancellationTokenSource(options.BatchMaxDelayMs);

                try
                {
                    while (batch.Count < options.BatchSize)
                        batch.Add(await reader.ReadAsync(window.Token));
                    await FlushAsync(batch);
                }
                catch (OperationCanceledException) // window expired
                {
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
            writeTracker.ReportSuccess();
            AggregatorMetrics.TicksWritten.Inc(inserted);
            // inserted < count => PK conflicts skipped by the DB safety net
            AggregatorMetrics.DbDuplicatesSkipped.Inc(batch.Count - inserted);
        }
        catch (Exception ex)
        {
            // Never silent: every lost tick is counted and logged.
            writeTracker.ReportFailure();
            AggregatorMetrics.TicksDropped.Inc(batch.Count);
            logger.LogError(ex, "batch of {Count} ticks dropped (DB write failed after retries or permanent error)", batch.Count);
        }
        finally
        {
            batch.Clear();
        }
    }
}
