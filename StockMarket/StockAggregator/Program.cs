using Prometheus;
using Serilog;
using StockAggregator.Connectors;
using StockAggregator.Hosting;
using StockAggregator.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));
builder.Services.AddAggregator(builder.Configuration);

// bounded shutdown budget for the drain chain
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));

var app = builder.Build();

// Liveness: process is up. Does not reflect DB or sources.
app.MapGet("/health", () => Results.Ok("ok"));

app.MapGet("/health/ready", (IDatabaseReadiness db, SourceStateTracker sources, DbWriteTracker writes) =>
{
    if (!db.Ready.IsCompletedSuccessfully)
        return Results.Json(new { status = "not_ready", reason = "database" }, statusCode: 503);

    if (!sources.AnyUp)
        return Results.Json(new { status = "not_ready", reason = "no_sources_up" }, statusCode: 503);

    if (writes.IsFailing(DbWriteTracker.FailingWindow))
        return Results.Json(new { status = "not_ready", reason = "db_writes_failing" }, statusCode: 503);

    return Results.Ok(new { status = "ready" });
});

app.MapMetrics();

app.Run();
