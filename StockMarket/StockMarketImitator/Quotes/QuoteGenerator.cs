namespace StockMarketImitator.Quotes;

// Called only from BroadcastEngine (single thread) no synchronization by design.
public sealed class QuoteGenerator
{
    private readonly string[] _tickers;
    private readonly Dictionary<string, decimal> _lastPrices = new();
    private long _seq;

    public QuoteGenerator(IEnumerable<string> tickers)
    {
        _tickers = tickers.ToArray();
        foreach (var t in _tickers)
            _lastPrices[t] = 100m + (decimal)Random.Shared.NextDouble() * 1000m;
    }

    public Quote Next()
    {
        var ticker = _tickers[Random.Shared.Next(_tickers.Length)];
        var price = _lastPrices[ticker];

        // random ±0.1% step around previous price
        var step = price * (decimal)(Random.Shared.NextDouble() - 0.5) * 0.002m;
        price = Math.Max(0.01m, price + step);
        _lastPrices[ticker] = price;

        var volume = Random.Shared.Next(1, 1000);

        return new Quote(
            Seq: ++_seq,
            Ticker: ticker,
            Price: Math.Round(price, 2),
            Volume: volume,
            TimestampUtc: DateTimeOffset.UtcNow);
    }
}
