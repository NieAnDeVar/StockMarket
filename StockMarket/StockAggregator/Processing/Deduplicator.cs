using System.Collections.Concurrent;
using StockAggregator.Contracts;

namespace StockAggregator.Processing;

// Key = (SourceId, SourceSeq). Content-key fallback is a conscious omission (see README).
// ConcurrentDictionary for O(1) lookup; ConcurrentQueue for approximate insertion order
// so eviction does not scan the whole dictionary. Timestamp in the dictionary is authoritative.
public sealed class Deduplicator(TimeSpan window)
{
    private readonly ConcurrentDictionary<(string Source, long Seq), long> _seen = new();
    private readonly ConcurrentQueue<((string Source, long Seq) Key, long Ticks)> _order = new();

    public TimeSpan Window { get; } = window;

    public bool IsNew(NormalizedTick tick)
    {
        var key = (tick.SourceId, tick.SourceSeq);
        var ticks = tick.TimestampUtc.UtcTicks;

        if (!_seen.TryAdd(key, ticks))
            return false;

        _order.Enqueue((key, ticks));
        return true;
    }

    public int EvictOlderThan(DateTimeOffset cutoffUtc)
    {
        var cutoff = cutoffUtc.UtcTicks;
        var removed = 0;

        while (_order.TryPeek(out var head) && head.Ticks < cutoff)
        {
            if (!_order.TryDequeue(out head))
                break;

            // re-check: a concurrent re-add may have a newer timestamp
            if (_seen.TryGetValue(head.Key, out var current) && current < cutoff
                && _seen.TryRemove(head.Key, out _))
            {
                removed++;
            }
        }

        return removed;
    }

    public int Count => _seen.Count;
}
