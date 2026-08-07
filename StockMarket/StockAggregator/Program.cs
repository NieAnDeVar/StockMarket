using Prometheus;
using Serilog;
using StockAggregator.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));
builder.Services.AddAggregator(builder.Configuration);

// bounded shutdown budget for the drain chain
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));

var app = builder.Build();

// liveness only; does not reflect DB readiness or source health
app.MapGet("/health", () => Results.Ok("ok"));
app.MapMetrics();

app.Run();
