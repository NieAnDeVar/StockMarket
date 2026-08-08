using Prometheus;

namespace StockAggregator.Observability;

// Labels carry source only (3 values); a ticker label would explode cardinality.
public static class AggregatorMetrics
{
    public static readonly Counter TicksReceived = Metrics.CreateCounter(
        "aggregator_ticks_received_total", "Raw messages received", "source");

    public static readonly Counter TicksNormalized = Metrics.CreateCounter(
        "aggregator_ticks_normalized_total", "Successfully normalized ticks");

    public static readonly Counter TicksDeduplicated = Metrics.CreateCounter(
        "aggregator_ticks_deduplicated_total", "Duplicates filtered by in-memory dedup");

    public static readonly Counter TicksWritten = Metrics.CreateCounter(
        "aggregator_ticks_written_total", "Rows inserted into the DB");

    public static readonly Counter TicksDropped = Metrics.CreateCounter(
        "aggregator_ticks_dropped_total", "Ticks lost after exhausted DB retries, must be 0 in steady state");

    public static readonly Counter DbDuplicatesSkipped = Metrics.CreateCounter(
        "aggregator_db_duplicates_skipped_total", "Rows skipped by DB ON CONFLICT, the safety net catching what memory missed");

    public static readonly Counter TicksMissed = Metrics.CreateCounter(
        "aggregator_ticks_missed_total", "Ticks that never arrived, detected by per-source seq gaps (reconnect window)", "source");

    public static readonly Counter ParseErrors = Metrics.CreateCounter(
        "aggregator_parse_errors_total", "Unparseable messages", "source");

    public static readonly Counter Reconnects = Metrics.CreateCounter(
        "aggregator_reconnects_total", "Reconnect attempts", "source");

    public static readonly Gauge SourceUp = Metrics.CreateGauge(
        "aggregator_source_up", "1 = connected, 0 = disconnected", "source");

    public static readonly Gauge ChannelOccupancy = Metrics.CreateGauge(
        "aggregator_channel_occupancy", "Channel fill, the backpressure indicator", "stage");

    public static readonly Gauge DedupCacheSize = Metrics.CreateGauge(
        "aggregator_dedup_cache_size", "Keys in the dedup window");

    public static readonly Histogram FeedLag = Metrics.CreateHistogram(
        "aggregator_feed_lag_ms", "Exchange timestamp -> arrival lag, ms",
        new HistogramConfiguration { Buckets = [1, 5, 10, 25, 50, 100, 250, 500, 1000] });
}
