namespace StockAggregator.Hosting;

// Runs several workers as one hosted service. Completes the shared channel only
// after ALL of them stopped — a single dying source must not close it for others.
public sealed class CompositeHostedService(
    IEnumerable<IHostedService> inner,
    Action? onAllStopped = null) : IHostedService
{
    public Task StartAsync(CancellationToken ct) =>
        Task.WhenAll(inner.Select(s => s.StartAsync(ct)));

    public async Task StopAsync(CancellationToken ct)
    {
        await Task.WhenAll(inner.Select(s => s.StopAsync(ct)));
        onAllStopped?.Invoke(); // drain chain starts here
    }
}
