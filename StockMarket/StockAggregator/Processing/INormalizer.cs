using StockAggregator.Contracts;

namespace StockAggregator.Processing;

public interface INormalizer
{
    /// <summary>Format key as used in SourceOptions.Format (e.g. "Alpha").</summary>
    string Format { get; }

    // Try-semantics: garbage in the stream is routine, not an exception
    bool TryNormalize(string raw, string sourceId, out NormalizedTick tick);
}
