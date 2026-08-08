namespace StockAggregator.Storage;

// Last DB write outcome, for readiness. A failure matters only while it is
// recent and nothing succeeded after it: one recovered batch clears the flag.
public sealed class DbWriteTracker
{
    public static readonly TimeSpan FailingWindow = TimeSpan.FromSeconds(30);

    private long _lastFailureTicks;
    private long _lastSuccessTicks;

    public void ReportSuccess() =>
        Interlocked.Exchange(ref _lastSuccessTicks, DateTimeOffset.UtcNow.UtcTicks);

    public void ReportFailure() =>
        Interlocked.Exchange(ref _lastFailureTicks, DateTimeOffset.UtcNow.UtcTicks);

    public bool IsFailing(TimeSpan window)
    {
        var failure = Interlocked.Read(ref _lastFailureTicks);
        if (failure == 0)
            return false;

        var success = Interlocked.Read(ref _lastSuccessTicks);
        return failure > success && DateTimeOffset.UtcNow.UtcTicks - failure < window.Ticks;
    }
}
