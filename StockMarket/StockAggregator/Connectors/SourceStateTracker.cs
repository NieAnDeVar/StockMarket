using System.Collections.Concurrent;

namespace StockAggregator.Connectors;

// Live connection state per source, written by the connectors.
// Readiness must not read its state through the metrics exporter.
public sealed class SourceStateTracker
{
    private readonly ConcurrentDictionary<string, bool> _up = new();

    public void Set(string sourceId, bool up) => _up[sourceId] = up;

    public bool AnyUp => _up.Values.Any(static v => v);
}
