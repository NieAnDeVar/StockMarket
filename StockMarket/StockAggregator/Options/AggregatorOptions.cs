using System.ComponentModel.DataAnnotations;

namespace StockAggregator.Options;

public sealed class AggregatorOptions
{
    public const string SectionName = "Aggregator";

    public List<SourceOptions> Sources { get; set; } = [];

    [Range(1, 1_000_000)]
    public int ChannelCapacity { get; set; } = 20_000;

    // Floors on every knob: 0 or negative would fail later and weirdly
    // (CTS(0) fires instantly, a negative timeout throws far from the cause).
    [Range(1, 300)]
    public int ConnectTimeoutSec { get; set; } = 10;

    // Silence on the wire == dead connection (half-open TCP doesn't report itself)
    [Range(1, 600)]
    public int IdleTimeoutSec { get; set; } = 15;

    // Batching: whichever fires first, size or time
    [Range(1, 100_000)]
    public int BatchSize { get; set; } = 500;

    [Range(1, 60_000)]
    public int BatchMaxDelayMs { get; set; } = 200;

    [Range(1, 3600)]
    public int DedupWindowSec { get; set; } = 60;

    // Must fail at startup, not in an endless migration retry loop
    [Required(AllowEmptyStrings = false)]
    public string ConnectionString { get; set; } = "";
}

public sealed class SourceOptions
{
    public string Id { get; set; } = "";
    public string Url { get; set; } = "";
    public string Format { get; set; } = ""; // Alpha | Beta | Gamma
}
