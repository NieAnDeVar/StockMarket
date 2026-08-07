using StockAggregator.Contracts;
using StockAggregator.Processing;

namespace StockAggregator.Tests;

public sealed class DeduplicatorTests
{
    private static NormalizedTick Tick(string source, long seq, DateTimeOffset? ts = null) =>
        new(source, seq, "AAPL", 187.3m, 100m, ts ?? DateTimeOffset.UtcNow);

    [Fact]
    public void SameSourceSameSeq_IsDuplicate()
    {
        var dedup = new Deduplicator(TimeSpan.FromSeconds(60));

        Assert.True(dedup.IsNew(Tick("alpha", 1)));
        Assert.False(dedup.IsNew(Tick("alpha", 1)));
    }

    [Fact]
    public void DifferentSourceSameSeq_IsNotDuplicate()
    {
        // Key includes the source: seq=1 from alpha and seq=1 from beta are different ticks
        var dedup = new Deduplicator(TimeSpan.FromSeconds(60));

        Assert.True(dedup.IsNew(Tick("alpha", 1)));
        Assert.True(dedup.IsNew(Tick("beta", 1)));
    }

    [Fact]
    public void EvictedKey_IsAcceptedAgain()
    {
        var dedup = new Deduplicator(TimeSpan.FromSeconds(60));
        var old = DateTimeOffset.UtcNow.AddHours(-2);

        Assert.True(dedup.IsNew(Tick("alpha", 1, old)));
        Assert.True(dedup.IsNew(Tick("alpha", 2)));

        var removed = dedup.EvictOlderThan(DateTimeOffset.UtcNow.AddHours(-1));

        Assert.Equal(1, removed);           // only the old key left the window
        Assert.True(dedup.IsNew(Tick("alpha", 1, old))); // may be re-accepted after eviction
        Assert.False(dedup.IsNew(Tick("alpha", 2)));     // recent one is still protected
    }

    // "Breaking" scenario #1 from the assignment: concurrent hammering with a 50% duplicate mix.
    [Fact]
    public void ConcurrentDuplicates_ExactlyUniquesPass()
    {
        var dedup = new Deduplicator(TimeSpan.FromMinutes(10));
        const int uniques = 50_000;
        var accepted = 0;

        // Feed every key twice, shuffled across threads, so each duplicate pair races itself.
        Parallel.For(0, uniques * 2, i =>
        {
            var seq = (i * 7919L) % uniques; // cheap deterministic shuffle
            if (dedup.IsNew(Tick("alpha", seq)))
                Interlocked.Increment(ref accepted);
        });

        Assert.Equal(uniques, accepted);
        Assert.Equal(uniques, dedup.Count);
    }

    [Fact]
    public void ConcurrentManySources_NoCrossTalk()
    {
        var dedup = new Deduplicator(TimeSpan.FromMinutes(10));
        string[] sources = ["alpha", "beta", "gamma"];
        const int perSource = 10_000;
        var accepted = 0;

        Parallel.For(0, sources.Length * perSource, i =>
        {
            var source = sources[i / perSource];
            var seq = i % perSource;
            if (dedup.IsNew(Tick(source, seq)))
                Interlocked.Increment(ref accepted);
        });

        // same seq range from 3 sources, all must pass: key is (source, seq)
        Assert.Equal(sources.Length * perSource, accepted);
    }
}
