using Microsoft.Extensions.Options;
using StockMarketImitator.Chaos;
using StockMarketImitator.Options;
using StockMarketImitator.Quotes;
using StockMarketImitator.Quotes.Formatters;
using StockMarketImitator.Streaming;

namespace StockMarketImitator.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddExchangeSimulator(
        this IServiceCollection services, IConfiguration config)
    {
        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen();

        services.AddOptions<SimulatorOptions>()
            .Bind(config.GetSection(SimulatorOptions.SectionName))
            // Invalid config must fail at startup
            .Validate(o => o.TicksPerSecond > 0, "TicksPerSecond must be positive")
            .Validate(o => o.Format is "Alpha" or "Beta" or "Gamma",
                "Format must be Alpha, Beta or Gamma")
            .ValidateOnStart();

        services.AddSingleton(sp =>
        {
            var opt = sp.GetRequiredService<IOptions<SimulatorOptions>>().Value;
            return new QuoteGenerator(opt.Tickers);
        });

        services.AddSingleton<IQuoteFormatter>(sp =>
        {
            var opt = sp.GetRequiredService<IOptions<SimulatorOptions>>().Value;
            return opt.Format switch
            {
                "Alpha" => new AlphaFormatter(),
                "Beta" => new BetaFormatter(),
                "Gamma" => new GammaFormatter(),
                var f => throw new InvalidOperationException($"Unknown format '{f}'")
            };
        });

        services.AddSingleton(sp =>
            new ChaosState(
                sp.GetRequiredService<IOptions<SimulatorOptions>>().Value.TicksPerSecond));

        services.AddSingleton<ClientRegistry>();
        services.AddSingleton<ClientSessionFactory>();

        services.AddSingleton<BroadcastEngine>();
        services.AddHostedService(sp => sp.GetRequiredService<BroadcastEngine>());

        return services;
    }
}
