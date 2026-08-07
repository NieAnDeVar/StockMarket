using Prometheus;
using Serilog;
using StockAggregator.Hosting;
using StockAggregator.Observability;
using StockAggregator.Options;
using StockAggregator.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));
builder.Services.AddAggregator(builder.Configuration);

// bounded shutdown budget for the drain chain
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));

var app = builder.Build();

// Liveness: process is up. Does not reflect DB or sources.
app.MapGet("/health", () => Results.Ok("ok"));

// Readiness: schema applied + at least one exchange source is currently connected.
// Used by orchestrators (k8s readinessProbe, compose healthcheck, etc.).
app.MapGet("/health/ready", (IDatabaseReadiness db, AggregatorOptions opts) =>
{
    if (!db.Ready.IsCompletedSuccessfully)
        return Results.Json(new { status = "not_ready", reason = "database" }, statusCode: 503);

    var anyUp = opts.Sources.Any(s =>
        AggregatorMetrics.SourceUp.WithLabels(s.Id).Value >= 1.0);

    if (!anyUp)
        return Results.Json(new { status = "not_ready", reason = "no_sources_up" }, statusCode: 503);

    return Results.Ok(new { status = "ready" });
});

app.MapMetrics();

app.Run();
