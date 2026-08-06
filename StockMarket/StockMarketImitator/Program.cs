using Prometheus;
using Serilog;
using StockMarketImitator.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));

builder.Services.AddExchangeSimulator(builder.Configuration);

var app = builder.Build();

app.UseWebSockets();
app.MapSimulatorEndpoints();

// Swagger оставляем включённым всегда: это наша "админка" для chaos-эндпоинтов.
app.UseSwagger();
app.UseSwaggerUI();

app.MapMetrics(); // prometheus-net: /metrics

app.Run();
