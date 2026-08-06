namespace StockAggregator.Contracts;

// Raw payload as received, before parsing. ReceivedAtUtc feeds feed-lag metrics later.
public sealed record IncomingTick(string SourceId, string Raw, DateTimeOffset ReceivedAtUtc);
