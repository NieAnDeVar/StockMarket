namespace StockMarketImitator.Quotes;

// Seq is a per-exchange monotonous id, lets consumers detect gaps and lets chaos tests verify "sent == received"
public sealed record Quote(
    long Seq,
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTimeOffset TimestampUtc);
