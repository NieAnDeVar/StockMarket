namespace StockMarketImitator.Options;

public sealed class SimulatorOptions
{
    public const string SectionName = "Simulator";

    public string ExchangeName { get; set; } = "alpha";
    public string Format { get; set; } = "Alpha"; // Alpha Beta Gamma
    public int TicksPerSecond { get; set; } = 350;
    public string[] Tickers { get; set; } = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"];
    public int HeartbeatIntervalSec { get; set; } = 5;
}
