using System.Globalization;
using System.Text.Json;

namespace StockMarketImitator.Quotes.Formatters;

// {"s":"AAPL","p":"187.30","v":100,"t":1754302530123,"n":1042} price as string, unix-ms time
public sealed class BetaFormatter : IQuoteFormatter
{
    public string Format(Quote q) => JsonSerializer.Serialize(new
    {
        s = q.Ticker,
        p = q.Price.ToString(CultureInfo.InvariantCulture),
        v = q.Volume,
        t = q.TimestampUtc.ToUnixTimeMilliseconds(), // regardless of OS locale
        n = q.Seq
    });
}
