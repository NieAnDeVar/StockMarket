using StockMarketImitator.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddExchangeSimulator(builder.Configuration);

var app = builder.Build();

app.UseWebSockets();
app.MapSimulatorEndpoints();

app.Run();
