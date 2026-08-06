namespace StockAggregator.Options;

public sealed class AggregatorOptions
{
    public const string SectionName = "Aggregator";

    public List<SourceOptions> Sources { get; set; } = [];
    public int ChannelCapacity { get; set; } = 20_000;
    public int ConnectTimeoutSec { get; set; } = 10;

    // Silence on the wire == dead connection (half-open TCP doesn't report itself)
    public int IdleTimeoutSec { get; set; } = 15;

    // Batching: whichever fires first — size or time
    public int BatchSize { get; set; } = 500;
    public int BatchMaxDelayMs { get; set; } = 200;

    public int DedupWindowSec { get; set; } = 60;

    public string ConnectionString { get; set; } = "";
}

public sealed class SourceOptions
{
    public string Id { get; set; } = "";
    public string Url { get; set; } = "";
    public string Format { get; set; } = ""; // Alpha | Beta | Gamma
}
