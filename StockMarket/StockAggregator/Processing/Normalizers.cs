using System.Globalization;
using System.Text.Json;
using StockAggregator.Contracts;

namespace StockAggregator.Processing;

public static class StreamMessage
{
    // Cheap pre-check before parsing: heartbeat carries no tick
    public static bool IsHeartbeat(string raw) => raw.Contains("\"heartbeat\"");
}

public sealed class AlphaNormalizer : INormalizer
{
    // {"symbol":"AAPL","price":187.3,"qty":100,"ts":"...Z","seq":1042}
    public bool TryNormalize(string raw, string sourceId, out NormalizedTick tick)
    {
        tick = default!;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var r = doc.RootElement;
            tick = new NormalizedTick(
                sourceId,
                r.GetProperty("seq").GetInt64(),
                r.GetProperty("symbol").GetString()!,
                r.GetProperty("price").GetDecimal(),
                r.GetProperty("qty").GetDecimal(),
                r.GetProperty("ts").GetDateTimeOffset());
            return true;
        }
        catch { return false; }
    }
}

public sealed class BetaNormalizer : INormalizer
{
    // {"s":"AAPL","p":"187.30","v":100,"t":1754302530123,"n":1042}
    public bool TryNormalize(string raw, string sourceId, out NormalizedTick tick)
    {
        tick = default!;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var r = doc.RootElement;

            // price arrives as a string; "." regardless of OS locale
            if (!decimal.TryParse(r.GetProperty("p").GetString(),
                    NumberStyles.Number, CultureInfo.InvariantCulture, out var price))
                return false;

            tick = new NormalizedTick(
                sourceId,
                r.GetProperty("n").GetInt64(),
                r.GetProperty("s").GetString()!,
                price,
                r.GetProperty("v").GetDecimal(),
                DateTimeOffset.FromUnixTimeMilliseconds(r.GetProperty("t").GetInt64()));
            return true;
        }
        catch { return false; }
    }
}

public sealed class GammaNormalizer : INormalizer
{
    // ["AAPL",187.3,100,1754302530,1042] — positional, unix seconds
    public bool TryNormalize(string raw, string sourceId, out NormalizedTick tick)
    {
        tick = default!;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var a = doc.RootElement;
            if (a.ValueKind != JsonValueKind.Array || a.GetArrayLength() != 5)
                return false;

            tick = new NormalizedTick(
                sourceId,
                a[4].GetInt64(),
                a[0].GetString()!,
                a[1].GetDecimal(),
                a[2].GetDecimal(),
                DateTimeOffset.FromUnixTimeSeconds(a[3].GetInt64()));
            return true;
        }
        catch { return false; }
    }
}
