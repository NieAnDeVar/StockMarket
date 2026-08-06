using StockMarketImitator.Chaos;

namespace StockMarketImitator.Tests;

public class ChaosStateTests
{
    [Fact]
    public void DuplicatesRate_IsClamped()
    {
        var chaos = new ChaosState(100);

        chaos.DuplicatesRate = 5;
        Assert.Equal(1, chaos.DuplicatesRate);

        chaos.DuplicatesRate = -1;
        Assert.Equal(0, chaos.DuplicatesRate);
    }

    [Fact]
    public void Rate_Zero_NeverDuplicates_Rate_One_AlwaysDuplicates()
    {
        var chaos = new ChaosState(100);

        chaos.DuplicatesRate = 0;
        Assert.DoesNotContain(Enumerable.Range(0, 100), _ => chaos.ShouldDuplicate());

        chaos.DuplicatesRate = 1;
        Assert.True(Enumerable.Range(0, 100).All(_ => chaos.ShouldDuplicate()));
    }

    [Fact]
    public void TicksPerSecond_HasSaneBounds()
    {
        var chaos = new ChaosState(100);

        chaos.TicksPerSecond = 0;
        Assert.Equal(1, chaos.TicksPerSecond);

        chaos.TicksPerSecond = int.MaxValue;
        Assert.Equal(50_000, chaos.TicksPerSecond);
    }
}
