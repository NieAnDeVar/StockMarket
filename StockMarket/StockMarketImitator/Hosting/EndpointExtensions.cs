using Microsoft.Extensions.Options;
using StockMarketImitator.Chaos;
using StockMarketImitator.Options;
using StockMarketImitator.Streaming;

namespace StockMarketImitator.Hosting;

public static class EndpointExtensions
{
    public static WebApplication MapSimulatorEndpoints(this WebApplication app)
    {
        app.Map("/ws", async (HttpContext ctx, ClientSessionFactory sessions,
                              ClientRegistry registry, IHostApplicationLifetime lifetime) =>
        {
            if (!ctx.WebSockets.IsWebSocketRequest)
            {
                ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
            await using var session = sessions.Create(socket);

            registry.Add(session);
            try
            {
                await session.RunAsync(lifetime.ApplicationStopping);
            }
            finally
            {
                registry.Remove(session.Id);
            }
        });

        app.MapPost("/chaos/disconnect", (ClientRegistry registry) =>
        {
            var sessions = registry.Snapshot();
            foreach (var s in sessions) s.Abort();
            return Results.Ok(new { disconnected = sessions.Count });
        });

        app.MapPost("/chaos/duplicates", (double rate, ChaosState chaos) =>
        {
            chaos.DuplicatesRate = rate;
            return Results.Ok(new { duplicatesRate = chaos.DuplicatesRate });
        });

        app.MapPost("/config/rate", (int ticksPerSecond, ChaosState chaos) =>
        {
            chaos.TicksPerSecond = ticksPerSecond;
            return Results.Ok(new { ticksPerSecond = chaos.TicksPerSecond });
        });

        app.MapGet("/status", (ClientRegistry registry, ChaosState chaos,
                               BroadcastEngine engine, IOptions<SimulatorOptions> opt) =>
            Results.Ok(new
            {
                exchange = opt.Value.ExchangeName,
                format = opt.Value.Format,
                clients = registry.Count,
                ticksPerSecond = chaos.TicksPerSecond,
                duplicatesRate = chaos.DuplicatesRate,
                sentTotal = engine.SentTotal
            }));

        app.MapGet("/health", () => Results.Ok("ok"));

        return app;
    }
}
