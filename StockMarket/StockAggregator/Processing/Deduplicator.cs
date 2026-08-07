using System.Collections.Concurrent;
using StockAggregator.Contracts;

namespace StockAggregator.Processing;

// Key = (SourceId, SourceSeq). Content-key fallback is documented in README as a conscious omission.
public sealed class Deduplicator(TimeSpan window)
{
    private readonly ConcurrentDictionary<(string Source, long Seq), long> _seen = new();

    public TimeSpan Window { get; } = window;

    public bool IsNew(NormalizedTick tick) =>
        _seen.TryAdd((tick.SourceId, tick.SourceSeq), tick.TimestampUtc.UtcTicks);

    public int EvictOlderThan(DateTimeOffset cutoffUtc)
    {
        var removed = 0;
        foreach (var (key, ticks) in _seen)
            if (ticks < cutoffUtc.UtcTicks && _seen.TryRemove(key, out _))
                removed++;
        return removed;
    }

    public int Count => _seen.Count;
}
