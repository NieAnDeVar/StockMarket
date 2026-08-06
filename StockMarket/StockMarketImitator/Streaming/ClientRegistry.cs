using System.Collections.Concurrent;

namespace StockMarketImitator.Streaming;

public sealed class ClientRegistry
{
    private readonly ConcurrentDictionary<Guid, ClientSession> _sessions = new();

    public void Add(ClientSession s)
    {
        _sessions[s.Id] = s;
        SimulatorMetrics.ClientsConnected.Inc();
    }

    public void Remove(Guid id)
    {
        if (_sessions.TryRemove(id, out _))
            SimulatorMetrics.ClientsConnected.Dec();
    }

    public IReadOnlyCollection<ClientSession> Snapshot() => _sessions.Values.ToArray();

    public int Count => _sessions.Count;
}
