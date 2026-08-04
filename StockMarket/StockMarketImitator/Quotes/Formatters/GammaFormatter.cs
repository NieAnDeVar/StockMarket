using System.Text.Json;

namespace StockMarketImitator.Quotes.Formatters;

// ["AAPL",187.3,100,1754302530,1042] array, unix seconds
public sealed class GammaFormatter : IQuoteFormatter
{
    public string Format(Quote q) => JsonSerializer.Serialize(new object[]
    {
        q.Ticker,
        q.Price,
        q.Volume,
        q.TimestampUtc.ToUnixTimeSeconds(),
        q.Seq
    });
}
