using System.Text.Json;

namespace StockAggregator.Processing;

public static class StreamMessage
{
    /// <summary>
    /// Heartbeat = JSON object with <c>"type":"heartbeat"</c> (case-insensitive).
    /// Full parse only runs when a "type" property is present at all.
    /// </summary>
    public static bool IsHeartbeat(string raw)
    {
        // ordinary ticks have no "type" property, skip the parser entirely
        if (string.IsNullOrEmpty(raw) || !raw.Contains("\"type\"", StringComparison.Ordinal))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return false;

            if (!doc.RootElement.TryGetProperty("type", out var typeProp))
                return false;

            return typeProp.ValueKind == JsonValueKind.String
                && typeProp.GetString()!.Equals("heartbeat", StringComparison.OrdinalIgnoreCase);
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
