using Prometheus;

namespace StockMarketImitator.Streaming;

// prometheus-net uses static metrics idiomatically: one registry per process.
public static class SimulatorMetrics
{
    public static readonly Counter TicksSent = Metrics.CreateCounter(
        "imitator_ticks_sent_total", "Ticks broadcast, including retransmitted duplicates");

    public static readonly Counter DuplicatesSent = Metrics.CreateCounter(
        "imitator_duplicates_sent_total", "Retransmitted duplicate ticks");

    public static readonly Counter DroppedForSlowClients = Metrics.CreateCounter(
        "imitator_ticks_dropped_total", "Ticks dropped for slow clients (channel overflow)");

    public static readonly Gauge ClientsConnected = Metrics.CreateGauge(
        "imitator_clients_connected", "Currently connected WebSocket clients");
}
