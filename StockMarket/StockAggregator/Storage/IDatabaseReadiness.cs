namespace StockAggregator.Storage;

// Readiness gate: the batch writer waits for the schema instead of failing first batches.
public interface IDatabaseReadiness
{
    Task Ready { get; }
}
