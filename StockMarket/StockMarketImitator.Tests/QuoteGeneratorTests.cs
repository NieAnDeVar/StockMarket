using StockMarketImitator.Quotes;

namespace StockMarketImitator.Tests;

public class QuoteGeneratorTests
{
    private static readonly string[] Tickers = ["AAPL", "MSFT"];

    [Fact]
    public void Seq_IncrementsStrictly()
    {
        var gen = new QuoteGenerator(Tickers);
        var seqs = Enumerable.Range(0, 100).Select(_ => gen.Next().Seq).ToArray();

        for (var i = 1; i < seqs.Length; i++)
            Assert.Equal(seqs[i - 1] + 1, seqs[i]);
    }

    [Fact]
    public void Prices_StayPositive_AndTickersFromList()
    {
        var gen = new QuoteGenerator(Tickers);

        for (var i = 0; i < 1000; i++)
        {
            var q = gen.Next();
            Assert.True(q.Price > 0);
            Assert.Contains(q.Ticker, Tickers);
            Assert.True(q.Volume > 0);
        }
    }

    [Fact]
    public void Price_MovesLikeRandomWalk_NotWhiteNoise()
    {
        var gen = new QuoteGenerator(Tickers);
        var prices = Enumerable.Range(0, 50)
            .Select(_ => gen.Next())
            .Where(q => q.Ticker == Tickers[0])
            .Select(q => q.Price)
            .ToArray();

        // соседние цены одного тикера отличаются не более чем на ~0.1% + округление
        for (var i = 1; i < prices.Length; i++)
            Assert.True(Math.Abs(prices[i] - prices[i - 1]) <= prices[i - 1] * 0.002m + 0.01m);
    }
}
