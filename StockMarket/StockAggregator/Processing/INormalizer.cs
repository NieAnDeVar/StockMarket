using StockAggregator.Contracts;
namespace StockAggregator.Processing;

public interface INormalizer
{
    // Try-semantics: garbage in the stream is routine, not an exception
    bool TryNormalize(string raw, string sourceId, out NormalizedTick tick);
}
