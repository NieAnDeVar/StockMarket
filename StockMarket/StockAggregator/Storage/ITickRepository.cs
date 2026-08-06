using StockAggregator.Contracts;

namespace StockAggregator.Storage;

public interface ITickRepository
{
    // Returns rows actually inserted; duplicates hit the PK and are skipped
    // (ON CONFLICT DO NOTHING) — the DB-level safety net behind in-memory dedup.
    Task<int> SaveBatchAsync(IReadOnlyList<NormalizedTick> batch, CancellationToken ct);
}
