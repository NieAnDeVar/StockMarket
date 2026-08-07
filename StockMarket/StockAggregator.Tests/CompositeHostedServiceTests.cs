using Microsoft.Extensions.Hosting;
using StockAggregator.Hosting;

namespace StockAggregator.Tests;

public sealed class CompositeHostedServiceTests
{
    private sealed class ProbeService : IHostedService
    {
        public int Starts, Stops;
        public Task StartAsync(CancellationToken ct) { Starts++; return Task.CompletedTask; }
        public Task StopAsync(CancellationToken ct) { Stops++; return Task.CompletedTask; }
    }

    // Regression: a lazy IEnumerable<IHostedService> (LINQ Select) re-evaluates per
    // enumeration, so StopAsync used to see fresh, never-started workers while the
    // real ones kept streaming.
    [Fact]
    public async Task LazyInnerEnumerable_IsMaterializedOnce()
    {
        var created = new List<ProbeService>();
        IEnumerable<IHostedService> Lazy() => Enumerable.Range(0, 3).Select(_ =>
        {
            var p = new ProbeService();
            created.Add(p);
            return p;
        });

        var completed = 0;
        var composite = new CompositeHostedService(Lazy(), () => completed++);

        await composite.StartAsync(CancellationToken.None);
        await composite.StopAsync(CancellationToken.None);

        Assert.Equal(3, created.Count);
        Assert.All(created, p => Assert.Equal(1, p.Starts));
        Assert.All(created, p => Assert.Equal(1, p.Stops));
        Assert.Equal(1, completed); // drain signal fired exactly once
    }

    [Fact]
    public async Task OnAllStopped_FiresOnlyAfterEveryWorkerStopped()
    {
        var probes = Enumerable.Range(0, 3).Select(_ => new ProbeService()).Cast<IHostedService>().ToArray();
        var stopsWhenFired = -1;
        var composite = new CompositeHostedService(probes,
            () => stopsWhenFired = probes.OfType<ProbeService>().Sum(p => p.Stops));

        await composite.StartAsync(CancellationToken.None);
        await composite.StopAsync(CancellationToken.None);

        Assert.Equal(3, stopsWhenFired);
    }
}
