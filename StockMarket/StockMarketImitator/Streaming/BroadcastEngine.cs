using System.Diagnostics;
using StockMarketImitator.Chaos;
using StockMarketImitator.Quotes;
using StockMarketImitator.Quotes.Formatters;

namespace StockMarketImitator.Streaming;

public sealed class BroadcastEngine(
    QuoteGenerator generator,
    IQuoteFormatter formatter,
    ClientRegistry registry,
    ChaosState chaos,
    ILogger<BroadcastEngine> logger) : BackgroundService
{
    private string? _lastMessage;

    private long _sentTotal;
    public long SentTotal => Interlocked.Read(ref _sentTotal);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Broadcast engine started");

        var lastIteration = Stopwatch.GetTimestamp();
        double credit = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(1), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            var now = Stopwatch.GetTimestamp();
            credit += Stopwatch.GetElapsedTime(lastIteration, now).TotalSeconds * chaos.TicksPerSecond;
            lastIteration = now;
            // catch-up after a stall is capped at 2s of traffic instead of one huge burst
            credit = Math.Min(credit, chaos.TicksPerSecond * 2.0);

            var due = (int)credit;
            if (due <= 0) continue;
            credit -= due;

            for (var i = 0; i < due; i++)
            {
                string message;
                if (_lastMessage is not null && chaos.ShouldDuplicate())
                {
                    message = _lastMessage; // retransmission: same seq, same payload
                    SimulatorMetrics.DuplicatesSent.Inc();
                }
                else
                {
                    message = formatter.Format(generator.Next());
                    _lastMessage = message;
                }

                Interlocked.Increment(ref _sentTotal);
                SimulatorMetrics.TicksSent.Inc();

                foreach (var session in registry.Snapshot())
                    session.Enqueue(message);
            }
        }

        foreach (var session in registry.Snapshot())
            session.Complete();

        logger.LogInformation("Broadcast engine stopped, sent total: {SentTotal}", SentTotal);
    }
}
