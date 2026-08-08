using System.Collections.Concurrent;

namespace StockAggregator.Processing;

// One connection streams one source in order, so a forward jump in seq means
// the ticks in between never arrived (reconnect window). Called only from
// the single processing loop. seq <= last is a duplicate or an exchange
// restarting its sequence: never counted, and the old max is kept.
public sealed class SeqGapTracker
{
    private readonly ConcurrentDictionary<string, long> _lastSeq = new();

    public long Observe(string sourceId, long seq)
    {
        if (!_lastSeq.TryGetValue(sourceId, out var last))
        {
            _lastSeq[sourceId] = seq; // first tick sets the baseline, not a gap
            return 0;
        }

        if (seq <= last)
            return 0;

        _lastSeq[sourceId] = seq;
        return seq - last - 1;
    }
}
