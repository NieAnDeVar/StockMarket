using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;
using StockAggregator.Storage;
using StockAggregator.Tests.Support;

namespace StockAggregator.Tests;

public sealed class TickBatchWriterTests
{
    private sealed class RecordingRepository : ITickRepository
    {
        private readonly object _gate = new();
        public List<int> BatchSizes = [];
        public readonly TaskCompletionSource FirstSaved =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Exception? Error;

        public Task<int> SaveBatchAsync(IReadOnlyList<NormalizedTick> batch, CancellationToken ct)
        {
            if (Error is not null) throw Error;
            lock (_gate) BatchSizes.Add(batch.Count);
            FirstSaved.TrySetResult();
            return Task.FromResult(batch.Count);
        }

        public int TotalSaved { get { lock (_gate) return BatchSizes.Sum(); } }
    }

    private sealed class InstantReadiness : IDatabaseReadiness
    {
        public Task Ready => Task.CompletedTask;
    }

    private static NormalizedTick Tick(long seq) =>
        new("alpha", seq, "AAPL", 187.3m, 100m, DateTimeOffset.UtcNow);

    private static (TickBatchWriter Worker, ChannelWriter<NormalizedTick> Writer, RecordingRepository Repo)
        StartWriter(int batchSize, int maxDelayMs)
    {
        var channel = Channel.CreateUnbounded<NormalizedTick>();
        var repo = new RecordingRepository();
        var options = new AggregatorOptions { BatchSize = batchSize, BatchMaxDelayMs = maxDelayMs };
        var worker = new TickBatchWriter(channel.Reader, repo, new InstantReadiness(), options,
            NullLogger<TickBatchWriter>.Instance);
        worker.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return (worker, channel.Writer, repo);
    }

    [Fact]
    public async Task SlowStream_FlushesByTimeNotBySize()
    {
        // 3 ticks << BatchSize: without a working time trigger they would wait forever
        var (worker, writer, repo) = StartWriter(batchSize: 500, maxDelayMs: 200);
        try
        {
            for (var i = 0; i < 3; i++) await writer.WriteAsync(Tick(i));

            await repo.FirstSaved.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await AsyncWait.UntilAsync(() => repo.TotalSaved == 3, TimeSpan.FromSeconds(2));
            Assert.Equal(3, repo.TotalSaved);
        }
        finally { writer.Complete(); await worker.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task FullBatch_FlushesBySizeImmediately()
    {
        // huge delay: if size trigger is broken, nothing flushes within the timeout
        var (worker, writer, repo) = StartWriter(batchSize: 10, maxDelayMs: 60_000);
        try
        {
            for (var i = 0; i < 10; i++) await writer.WriteAsync(Tick(i));

            await repo.FirstSaved.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(10, repo.BatchSizes[0]);
        }
        finally { writer.Complete(); await worker.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task ChannelCompleted_DrainFlushesRemainder()
    {
        var (worker, writer, repo) = StartWriter(batchSize: 500, maxDelayMs: 60_000);
        for (var i = 0; i < 7; i++) await writer.WriteAsync(Tick(i));
        writer.Complete(); // graceful shutdown: nothing in memory may be lost

        // The writer finishes on channel completion by itself, drain needs no host stop.
        await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(7, repo.TotalSaved);
    }

    [Fact]
    public async Task PermanentDbError_IsCountedNotSilent()
    {
        var (worker, writer, repo) = StartWriter(batchSize: 500, maxDelayMs: 200);
        try
        {
            repo.Error = new InvalidOperationException("permanent failure");
            var droppedBefore = AggregatorMetrics.TicksDropped.Value;

            for (var i = 0; i < 5; i++) await writer.WriteAsync(Tick(i));

            var counted = await AsyncWait.UntilAsync(
                () => AggregatorMetrics.TicksDropped.Value - droppedBefore >= 5,
                TimeSpan.FromSeconds(5));

            Assert.True(counted, "dropped ticks must be counted, not swallowed");
        }
        finally { writer.Complete(); await worker.StopAsync(CancellationToken.None); }
    }
}
