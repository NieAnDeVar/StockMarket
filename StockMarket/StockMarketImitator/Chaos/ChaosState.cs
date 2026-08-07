namespace StockMarketImitator.Chaos;

public sealed class ChaosState
{
    // double can't be volatile, so writes go through Interlocked.Exchange and
    // reads through CompareExchange(ref, 0, 0): both are atomic for 64-bit values.
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
