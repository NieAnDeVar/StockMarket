using System.Collections.Concurrent;
using StockAggregator.Contracts;

namespace StockAggregator.Processing;

// Key = (SourceId, SourceSeq): exchanges retransmit the same tick with the same seq.
// For a source without its own ids the fallback is a content key — noted in README.
public sealed class Deduplicator(TimeSpan window)
{
    private readonly ConcurrentDictionary<(string Source, long Seq), long> _seen = new();

    public TimeSpan Window { get; } = window;

    // Single atomic check-and-add: true = first time we see the tick.
    public bool IsNew(NormalizedTick tick) =>
        _seen.TryAdd((tick.SourceId, tick.SourceSeq), tick.TimestampUtc.UtcTicks);

    // Sliding window eviction; ConcurrentDictionary tolerates concurrent reads during it.
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
