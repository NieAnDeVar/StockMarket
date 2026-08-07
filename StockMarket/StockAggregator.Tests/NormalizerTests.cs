using System.Globalization;
using StockAggregator.Processing;

namespace StockAggregator.Tests;

public sealed class NormalizerTests
{
    [Fact]
    public void Alpha_ParsesContract()
    {
        var raw = "{\"symbol\":\"AAPL\",\"price\":187.3,\"qty\":100,\"ts\":\"2026-01-05T12:30:00Z\",\"seq\":1042}";

        Assert.True(new AlphaNormalizer().TryNormalize(raw, "alpha", out var tick));

        Assert.Equal("alpha", tick.SourceId);
        Assert.Equal(1042, tick.SourceSeq);
        Assert.Equal("AAPL", tick.Ticker);
        Assert.Equal(187.3m, tick.Price);
        Assert.Equal(100m, tick.Volume);
        Assert.Equal(DateTimeOffset.Parse("2026-01-05T12:30:00Z"), tick.TimestampUtc);
    }

    [Fact]
    public void Beta_ParsesContract()
    {
        var raw = "{\"s\":\"AAPL\",\"p\":\"187.30\",\"v\":100,\"t\":1754302530123,\"n\":7}";

        Assert.True(new BetaNormalizer().TryNormalize(raw, "beta", out var tick));

        Assert.Equal(7, tick.SourceSeq);
        Assert.Equal("AAPL", tick.Ticker);
        Assert.Equal(187.30m, tick.Price);
        Assert.Equal(100m, tick.Volume);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1754302530123), tick.TimestampUtc);
    }

    [Fact]
    public void Beta_PriceStringIgnoresOsLocale()
    {
        // ru-RU uses "," as decimal separator, "187.30" must still parse as 187.30
        var previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("ru-RU");
        try
        {
            var raw = "{\"s\":\"AAPL\",\"p\":\"187.30\",\"v\":100,\"t\":1754302530123,\"n\":7}";
            Assert.True(new BetaNormalizer().TryNormalize(raw, "beta", out var tick));
            Assert.Equal(187.30m, tick.Price);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void Gamma_ParsesPositionalArray()
    {
        var raw = "[\"AAPL\",187.3,100,1754302530,1042]";

        Assert.True(new GammaNormalizer().TryNormalize(raw, "gamma", out var tick));

        Assert.Equal(1042, tick.SourceSeq);
        Assert.Equal("AAPL", tick.Ticker);
        Assert.Equal(187.3m, tick.Price);
        Assert.Equal(100m, tick.Volume);
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1754302530), tick.TimestampUtc);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not json at all")]
    [InlineData("{}")]
    [InlineData("{\"symbol\":\"AAPL\"}")]               // missing most fields
    [InlineData("{\"symbol\":\"AAPL\",\"price\":\"oops\",\"qty\":100,\"ts\":\"2026-01-05T12:30:00Z\",\"seq\":1042}")]
    public void Alpha_BrokenInput_ReturnsFalseWithoutThrowing(string raw)
    {
        Assert.False(new AlphaNormalizer().TryNormalize(raw, "alpha", out _));
    }

    [Theory]
    [InlineData("[\"AAPL\",187.3,100,1754302530]")]        // one field short
    [InlineData("[\"AAPL\",187.3,100,1754302530,1042,99]")] // one too many
    [InlineData("{\"s\":\"AAPL\"}")]                        // object instead of array
    public void Gamma_WrongShape_ReturnsFalse(string raw)
    {
        Assert.False(new GammaNormalizer().TryNormalize(raw, "gamma", out _));
    }

    [Fact]
    public void Garbage_NeverThrows()
    {
        INormalizer[] normalizers = [new AlphaNormalizer(), new BetaNormalizer(), new GammaNormalizer()];
        var rng = new Random(42);

        for (var i = 0; i < 1000; i++)
        {
            var length = rng.Next(0, 200);
            var chars = new char[length];
            for (var j = 0; j < length; j++)
                chars[j] = (char)rng.Next(32, 127);
            var garbage = new string(chars);

            foreach (var n in normalizers)
                n.TryNormalize(garbage, "test", out _); // must simply return false
        }
    }

    [Fact]
    public void Heartbeat_IsDetectedBeforeParsing()
    {
        Assert.True(StreamMessage.IsHeartbeat("{\"type\":\"heartbeat\",\"ts\":\"2026-01-01T00:00:00Z\"}"));
        Assert.False(StreamMessage.IsHeartbeat(
            "{\"symbol\":\"AAPL\",\"price\":187.3,\"qty\":100,\"ts\":\"2026-01-05T12:30:00Z\",\"seq\":1042}"));
    }
}
