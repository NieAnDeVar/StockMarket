using Prometheus;
using Serilog;
using StockAggregator.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));
builder.Services.AddAggregator(builder.Configuration);

// Drain gets a bounded, visible budget — not infinite, not zero.
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("ok"));
app.MapMetrics();

app.Run();
