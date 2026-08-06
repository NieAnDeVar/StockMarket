namespace StockAggregator.Contracts;

// Single internal shape. SourceSeq is the exchange's own id — gap/dup detection lives on it.
public sealed record NormalizedTick(
    string SourceId,
    long SourceSeq,
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTimeOffset TimestampUtc);
