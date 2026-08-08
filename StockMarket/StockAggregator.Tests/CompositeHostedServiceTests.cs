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

    // A worker whose StopAsync returns while ExecuteTask is still running.
    // .NET 9+ makes this the normal case: ExecuteAsync is scheduled Task.Run
    // and the host does not wait for it before reporting stopped.
    private sealed class SlowToFinishWorker : BackgroundService
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Entered => _entered.Task;
        public int Exited;

        public void Release() => _release.TrySetResult();

        public override Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _entered.TrySetResult();
            await _release.Task;
            Exited++;
        }
    }

    [Fact]
    public async Task StopAsync_WaitsForExecuteTaskEvenWhenWorkerStopReturned()
    {
        var worker = new SlowToFinishWorker();
        var exitedWhenFired = -1;
        var composite = new CompositeHostedService(new IHostedService[] { worker },
            () => exitedWhenFired = worker.Exited);

        await composite.StartAsync(CancellationToken.None);
        await worker.Entered.WaitAsync(TimeSpan.FromSeconds(5));

        var stopTask = composite.StopAsync(CancellationToken.None);
        await Task.Delay(200);
        Assert.False(stopTask.IsCompleted); // ExecuteTask still blocked, composite must wait

        worker.Release();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, worker.Exited);
        Assert.Equal(1, exitedWhenFired); // drain signal fired only after ExecuteTask finished
    }
}
