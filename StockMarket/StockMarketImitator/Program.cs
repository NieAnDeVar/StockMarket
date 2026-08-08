using Prometheus;
using Serilog;
using StockMarketImitator.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));

builder.Services.AddExchangeSimulator(builder.Configuration);

var app = builder.Build();

app.UseWebSockets();
app.MapSimulatorEndpoints();

// Swagger stays enabled: it is the admin UI for the chaos endpoints.
app.UseSwagger();
app.UseSwaggerUI();

app.MapMetrics();

app.Run();
