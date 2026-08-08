using StockAggregator.Storage;

namespace StockAggregator.Tests;

public sealed class DbWriteTrackerTests
{
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    [Fact]
    public void NoFailures_NotFailing()
    {
        Assert.False(new DbWriteTracker().IsFailing(Window));
    }

    [Fact]
    public void RecentFailureWithoutLaterSuccess_Failing()
    {
        var tracker = new DbWriteTracker();
        tracker.ReportFailure();

        Assert.True(tracker.IsFailing(Window));
    }

    [Fact]
    public void SuccessAfterFailure_ClearsFailing()
    {
        var tracker = new DbWriteTracker();
        tracker.ReportFailure();
        tracker.ReportSuccess();

        Assert.False(tracker.IsFailing(Window));
    }
}
