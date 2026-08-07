namespace StockAggregator.Storage;

// Readiness gate for writers: the batch writer waits for the schema instead of
// failing its first batches while the DB is still booting.
public interface IDatabaseReadiness
{
    Task Ready { get; }
}
