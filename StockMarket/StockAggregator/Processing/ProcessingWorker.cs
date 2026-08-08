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
    private readonly SeqGapTracker _gapTracker;
    private readonly Dictionary<string, INormalizer> _normalizers;

    public ProcessingWorker(
        ChannelReader<IncomingTick> reader,
        ChannelWriter<NormalizedTick> normalizedWriter,
        Deduplicator dedup,
        SeqGapTracker gapTracker,
        AggregatorOptions options,
        IEnumerable<INormalizer> normalizers)
    {
        _reader = reader;
        _normalizedWriter = normalizedWriter;
        _dedup = dedup;
        _gapTracker = gapTracker;

        var byFormat = normalizers.ToDictionary(
            n => n.Format,
            n => n,
            StringComparer.OrdinalIgnoreCase);

        _normalizers = options.Sources.ToDictionary(
            s => s.Id,
            s =>
            {
                if (!byFormat.TryGetValue(s.Format, out var n))
                    throw new InvalidOperationException(
                        $"No INormalizer registered for format '{s.Format}' (source '{s.Id}'). " +
                        "Register a new implementation and add it in DI.");
                return n;
            });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // ends on channel completion, not host token (otherwise middle dies before head)
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

            var missed = _gapTracker.Observe(incoming.SourceId, tick.SourceSeq);
            if (missed > 0)
                AggregatorMetrics.TicksMissed.WithLabels(incoming.SourceId).Inc(missed);

            if (!_dedup.IsNew(tick))
            {
                AggregatorMetrics.TicksDeduplicated.Inc();
                continue;
            }

            AggregatorMetrics.TicksNormalized.Inc();
            await _normalizedWriter.WriteAsync(tick);
        }

        _normalizedWriter.Complete();
    }
}
