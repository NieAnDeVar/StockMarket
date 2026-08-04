namespace StockMarketImitator.Chaos;

public sealed class ChaosState
{
    // double can't be volatile and Interlocked.Read has no double overload
    // writes go through Exchange, reads through CompareExchange both are atomic.
    private double _duplicatesRate;

    // volatile: written from HTTP threads, read by the engine thread
    private volatile int _ticksPerSecond;

    public ChaosState(int initialRate) => _ticksPerSecond = initialRate;

    public double DuplicatesRate
    {
        get => Interlocked.CompareExchange(ref _duplicatesRate, 0, 0);
        set => Interlocked.Exchange(ref _duplicatesRate, Math.Clamp(value, 0, 1));
    }

    public int TicksPerSecond
    {
        get => _ticksPerSecond;
        set => _ticksPerSecond = Math.Clamp(value, 1, 50_000);
    }

    public bool ShouldDuplicate() => Random.Shared.NextDouble() < DuplicatesRate;
}
