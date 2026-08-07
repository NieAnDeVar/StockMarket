namespace StockAggregator.Hosting;

// Completes the shared channel only after ALL workers stopped.
public sealed class CompositeHostedService : IHostedService
{
    private readonly IReadOnlyList<IHostedService> _inner;
    private readonly Action? _onAllStopped;

    public CompositeHostedService(IEnumerable<IHostedService> inner, Action? onAllStopped = null)
    {
        // materialize: lazy LINQ would re-enumerate on StopAsync and produce new never-started workers
        _inner = inner as IReadOnlyList<IHostedService> ?? inner.ToArray();
        _onAllStopped = onAllStopped;
    }

    public Task StartAsync(CancellationToken ct) =>
        Task.WhenAll(_inner.Select(s => s.StartAsync(ct)));

    public async Task StopAsync(CancellationToken ct)
    {
        await Task.WhenAll(_inner.Select(s => s.StopAsync(ct)));

        // .NET 9+: StopAsync can return before ExecuteTask finishes; drain must wait for the tasks
        var executeTasks = _inner.OfType<BackgroundService>()
            .Select(s => s.ExecuteTask ?? Task.CompletedTask);
        try { await Task.WhenAll(executeTasks); }
        catch { /* already logged by workers */ }

        _onAllStopped?.Invoke();
    }
}
