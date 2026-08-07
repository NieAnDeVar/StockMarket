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
            .Validate(o => o.Sources.All(s => Uri.IsWellFormedUriString(s.Url, UriKind.Absolute)),
                "every source needs an absolute ws:// url")
            .ValidateOnStart();

        services.AddSingleton(sp => sp.GetRequiredService<IOptions<AggregatorOptions>>().Value);

        // one channel instance exposed as channel / reader / writer
        services.AddSingleton(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            return Channel.CreateBounded<IncomingTick>(new BoundedChannelOptions(opt.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true
            });
        });
        services.AddSingleton(sp => sp.GetRequiredService<Channel<IncomingTick>>().Reader);
        services.AddSingleton(sp => sp.GetRequiredService<Channel<IncomingTick>>().Writer);

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

        // also implements IDatabaseReadiness for the batch writer
        services.AddSingleton(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            return new DatabaseInitializer(
                opt.ConnectionString,
                sp.GetRequiredService<ILogger<DatabaseInitializer>>());
        });
        services.AddHostedService(sp => sp.GetRequiredService<DatabaseInitializer>());
        services.AddSingleton<IDatabaseReadiness>(sp => sp.GetRequiredService<DatabaseInitializer>());

        // LIFO shutdown: connectors registered last → stop first
        services.AddHostedService<MetricsWorker>();
        services.AddHostedService(sp => new DedupCleanupWorker(
            sp.GetRequiredService<Deduplicator>(),
            sp.GetRequiredService<ILogger<DedupCleanupWorker>>()));
        services.AddHostedService<TickBatchWriter>();
        services.AddHostedService<ProcessingWorker>();

        // only stage that observes host cancellation; onAllStopped completes the raw channel
        services.AddHostedService(sp =>
        {
            var opt = sp.GetRequiredService<AggregatorOptions>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var writer = sp.GetRequiredService<ChannelWriter<IncomingTick>>();
            var connectors = opt.Sources.Select(s =>
                (IHostedService)new ExchangeConnectorWorker(
                    s, writer, opt,
                    loggerFactory.CreateLogger<ExchangeConnectorWorker>())).ToArray();
            return new CompositeHostedService(connectors, onAllStopped: () => writer.Complete());
        });

        return services;
    }
}
