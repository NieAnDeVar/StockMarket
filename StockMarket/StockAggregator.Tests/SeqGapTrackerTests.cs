using StockAggregator.Processing;

namespace StockAggregator.Tests;

public sealed class SeqGapTrackerTests
{
    [Fact]
    public void ForwardJump_ReportsExactlyTheMissingTicks()
    {
        var tracker = new SeqGapTracker();

        Assert.Equal(0, tracker.Observe("alpha", 1)); // first tick: baseline, not a gap
        Assert.Equal(0, tracker.Observe("alpha", 2));
        Assert.Equal(2, tracker.Observe("alpha", 5)); // 3 and 4 never arrived
        Assert.Equal(0, tracker.Observe("alpha", 6));
    }

    [Fact]
    public void DuplicatesAndSequenceRestarts_AreNotGaps()
    {
        var tracker = new SeqGapTracker();

        Assert.Equal(0, tracker.Observe("alpha", 10));
        Assert.Equal(0, tracker.Observe("alpha", 10)); // retransmission
        Assert.Equal(0, tracker.Observe("alpha", 1));  // exchange restarted its sequence
        Assert.Equal(0, tracker.Observe("beta", 10));  // sources are independent
        Assert.Equal(0, tracker.Observe("beta", 11));
    }
}
