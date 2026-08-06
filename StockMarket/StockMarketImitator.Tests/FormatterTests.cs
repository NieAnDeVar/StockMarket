using System.Text.Json;
using StockMarketImitator.Quotes;
using StockMarketImitator.Quotes.Formatters;

namespace StockMarketImitator.Tests;

public class FormatterTests
{
    private static readonly Quote Tick = new(
        Seq: 1042, Ticker: "AAPL", Price: 187.30m, Volume: 100,
        TimestampUtc: new DateTimeOffset(2026, 8, 4, 10, 15, 30, TimeSpan.Zero));

    [Fact]
    public void Alpha_HasNamedFields_NumberPrice_IsoTime()
    {
        using var doc = JsonDocument.Parse(new AlphaFormatter().Format(Tick));
        var root = doc.RootElement;

        Assert.Equal("AAPL", root.GetProperty("symbol").GetString());
        Assert.Equal(187.30m, root.GetProperty("price").GetDecimal()); // число, не строка
        Assert.Equal(100, root.GetProperty("qty").GetDecimal());
        Assert.Equal(1042, root.GetProperty("seq").GetInt64());
        Assert.True(root.GetProperty("ts").GetDateTimeOffset().Year == 2026);
    }

    [Fact]
    public void Beta_PriceIsStringWithDot_TimeIsUnixMs()
    {
        using var doc = JsonDocument.Parse(new BetaFormatter().Format(Tick));
        var root = doc.RootElement;

        var price = root.GetProperty("p");
        Assert.Equal(JsonValueKind.String, price.ValueKind); // цена именно строкой
        Assert.Equal("187.30", price.GetString());           // и с точкой, не запятой
        Assert.Equal(Tick.TimestampUtc.ToUnixTimeMilliseconds(), root.GetProperty("t").GetInt64());
        Assert.Equal(1042, root.GetProperty("n").GetInt64());
    }

    [Fact]
    public void Gamma_PositionalArray_UnixSeconds()
    {
        using var doc = JsonDocument.Parse(new GammaFormatter().Format(Tick));
        var arr = doc.RootElement;

        Assert.Equal(JsonValueKind.Array, arr.ValueKind);
        Assert.Equal(5, arr.GetArrayLength());
        Assert.Equal("AAPL", arr[0].GetString());
        Assert.Equal(187.30m, arr[1].GetDecimal());
        Assert.Equal(Tick.TimestampUtc.ToUnixTimeSeconds(), arr[3].GetInt64()); // секунды, не мс
        Assert.Equal(1042, arr[4].GetInt64());
    }
}
