using System.Threading.Channels;
using Microsoft.Extensions.Options;
using StockAggregator.Connectors;
using StockAggregator.Contracts;
using StockAggregator.Observability;
using StockAggregator.Options;
using StockAggregator.Processing;
using StockAggregator.Storage;

namespace StockAggregator.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAggregator(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<AggregatorOptions>()
            .Bind(configuration.GetSection(AggregatorOptions.SectionName))
            .ValidateDataAnnotations()
            .Validate(o => o.Sources.Count > 0, "at least one source is required")
            .ValidateOnStart();

        // lets services take AggregatorOptions directly instead of the IOptions<> wrapper
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<AggregatorOptions>>().Value);

        // Both channels are registered three ways — channel, reader, writer — all pointing
        // at the same instance. Creating channels separately for reader/writer would wire
        // producers and consumers to different channels and stall the pipeline silently.
        services.AddSingleton(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            return Channel.CreateBounded<IncomingTick>(new BoundedChannelOptions(opt.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait, // backpressure: slow consumer throttles the sockets
                SingleReader = true
            });
        });
        services.AddSingleton(sp => sp.GetRequiredService<Channel<IncomingTick>>().Reader);
        services.AddSingleton(sp => sp.GetRequiredService<Channel<IncomingTick>>().Writer);

        // Bounded like the raw one: a slow DB must push back through the whole pipeline,
        // not inflate memory in the middle of it.
        services.AddSingleton(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            return Channel.CreateBounded<NormalizedTick>(new BoundedChannelOptions(opt.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true
            });
        });
        services.AddSingleton(sp => sp.GetRequiredService<Channel<NormalizedTick>>().Reader);
        services.AddSingleton(sp => sp.GetRequiredService<Channel<NormalizedTick>>().Writer);

        // Primitives in a constructor (TimeSpan, string) are not services —
        // such types are registered via factory, values come from options.
        services.AddSingleton(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            return new Deduplicator(TimeSpan.FromSeconds(opt.DedupWindowSec));
        });

        services.AddSingleton<ITickRepository>(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            return new PostgresTickRepository(
                opt.ConnectionString,
                sp.GetRequiredService<ILogger<PostgresTickRepository>>());
        });

        // One instance serves both as the hosted migration runner and as the
        // readiness gate (dbReady.Ready) for the batch writer.
        services.AddSingleton(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            return new DatabaseInitializer(
                opt.ConnectionString,
                sp.GetRequiredService<ILogger<DatabaseInitializer>>());
        });
        services.AddHostedService(sp => sp.GetRequiredService<DatabaseInitializer>());

        // Connectors are the only stage listening to the host cancellation token.
        // When ALL of them are down, the raw channel completes and the drain chain
        // unwinds downstream on its own — processing finishes, then the batch writer flushes.
        services.AddHostedService(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var writer = sp.GetRequiredService<ChannelWriter<IncomingTick>>();
            var connectors = opt.Sources.Select(s =>
                (IHostedService)new ExchangeConnectorWorker(
                    s,
                    writer,
                    opt,
                    loggerFactory.CreateLogger<ExchangeConnectorWorker>()));
            return new CompositeHostedService(connectors, onAllStopped: () => writer.Complete());
        });

        services.AddHostedService<ProcessingWorker>();
        services.AddHostedService<TickBatchWriter>();

        services.AddHostedService(sp => new DedupCleanupWorker(
            sp.GetRequiredService<Deduplicator>(),
            sp.GetRequiredService<ILogger<DedupCleanupWorker>>()));

        services.AddHostedService<MetricsWorker>();

        return services;
    }
}
