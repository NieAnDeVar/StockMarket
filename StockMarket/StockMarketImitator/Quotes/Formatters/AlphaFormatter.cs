using System.Text.Json;

namespace StockMarketImitator.Quotes.Formatters;

// {"symbol":"AAPL","price":187.3,"qty":100,"ts":"...Z","seq":1042}
public sealed class AlphaFormatter : IQuoteFormatter
{
    public string Format(Quote q) => JsonSerializer.Serialize(new
    {
        symbol = q.Ticker,
        price = q.Price,
        qty = q.Volume,
        ts = q.TimestampUtc.ToString("O"),
        seq = q.Seq
    });
}
