using System.Net.WebSockets;
using Microsoft.Extensions.Options;
using StockMarketImitator.Options;

namespace StockMarketImitator.Streaming;

public sealed class ClientSessionFactory(IOptions<SimulatorOptions> options)
{
    public ClientSession Create(WebSocket socket) =>
        new(socket, TimeSpan.FromSeconds(options.Value.HeartbeatIntervalSec));
}
