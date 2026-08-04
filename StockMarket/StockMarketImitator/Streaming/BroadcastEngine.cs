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

        while (!stoppingToken.IsCancellationRequested)
        {
            // Delay is re-read every iteration so /config/rate takes effect immediately
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(1.0 / chaos.TicksPerSecond), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            string message;
            if (_lastMessage is not null && chaos.ShouldDuplicate())
            {
                message = _lastMessage; // retransmission: same seq, same payload
            }
            else
            {
                message = formatter.Format(generator.Next());
                _lastMessage = message;
            }

            Interlocked.Increment(ref _sentTotal);

            foreach (var session in registry.Snapshot())
                session.Enqueue(message);
        }

        foreach (var session in registry.Snapshot())
            session.Complete();

        logger.LogInformation("Broadcast engine stopped, sent total: {SentTotal}", SentTotal);
    }
}
