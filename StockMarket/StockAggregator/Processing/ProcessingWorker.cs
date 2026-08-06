using System.Threading.Channels;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;

namespace StockAggregator.Processing;

public sealed class ProcessingWorker : BackgroundService
{
    private readonly ChannelReader<IncomingTick> _reader;
    private readonly ChannelWriter<NormalizedTick> _normalizedWriter;
    private readonly Deduplicator _dedup;
    private readonly Dictionary<string, INormalizer> _normalizers;

    public ProcessingWorker(
        ChannelReader<IncomingTick> reader,
        ChannelWriter<NormalizedTick> normalizedWriter,
        Deduplicator dedup,
        AggregatorOptions options)
    {
        _reader = reader;
        _normalizedWriter = normalizedWriter;
        _dedup = dedup;

        // SourceId -> normalizer, built once from config.
        // New exchange = new INormalizer + one config entry. Nothing here changes.
        _normalizers = options.Sources.ToDictionary(
            s => s.Id,
            s => (INormalizer)(s.Format switch
            {
                "Alpha" => new AlphaNormalizer(),
                "Beta" => new BetaNormalizer(),
                "Gamma" => new GammaNormalizer(),
                var f => throw new InvalidOperationException($"Unknown format '{f}' for source '{s.Id}'")
            }));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // No stoppingToken in the read loop on purpose: this stage ends when
        // connectors Complete the channel, not when the host fires cancellation.
        // Otherwise the middle of the pipeline would die before the head — losing the tail.
        await foreach (var incoming in _reader.ReadAllAsync())
        {
            AggregatorMetrics.TicksReceived.WithLabels(incoming.SourceId).Inc();

            if (StreamMessage.IsHeartbeat(incoming.Raw))
                continue;

            if (!_normalizers[incoming.SourceId].TryNormalize(incoming.Raw, incoming.SourceId, out var tick))
            {
                AggregatorMetrics.ParseErrors.WithLabels(incoming.SourceId).Inc();
                continue;
            }

            AggregatorMetrics.FeedLag.Observe(
                (incoming.ReceivedAtUtc - tick.TimestampUtc).TotalMilliseconds);

            if (!_dedup.IsNew(tick))
            {
                AggregatorMetrics.TicksDeduplicated.Inc();
                continue;
            }

            AggregatorMetrics.TicksNormalized.Inc();
            await _normalizedWriter.WriteAsync(tick);
        }

        // Raw input ended -> close the next stage so the batch writer drains and flushes.
        _normalizedWriter.Complete();
    }
}
