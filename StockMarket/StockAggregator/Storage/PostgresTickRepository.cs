using System.Net.Sockets;
using Npgsql;
using NpgsqlTypes;
using StockAggregator.Contracts;

namespace StockAggregator.Storage;

public sealed class PostgresTickRepository(string connectionString, ILogger<PostgresTickRepository> logger)
    : ITickRepository
{
    private const int MaxAttempts = 5;

    public async Task<int> SaveBatchAsync(IReadOnlyList<NormalizedTick> batch, CancellationToken ct)
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                return await SaveOnceAsync(batch, ct);
            }
            // Retry only transient failures; retrying a permanent error just delays the drop.
            catch (Exception ex) when (IsTransient(ex) && attempt < MaxAttempts)
            {
                attempt++;
                var delay = TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt));
                logger.LogWarning(ex, "db save failed, attempt {Attempt}/{Max}, retry in {Delay}ms",
                    attempt, MaxAttempts, (int)delay.TotalMilliseconds);
                await Task.Delay(delay, ct);
            }
        }
    }

    private async Task<int> SaveOnceAsync(IReadOnlyList<NormalizedTick> batch, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // COPY has no ON CONFLICT: bulk-load into a temp table, then insert-select
        // with conflict handling, one transaction.
        await using (var create = new NpgsqlCommand(
            "CREATE TEMP TABLE IF NOT EXISTS temp_ticks (LIKE ticks INCLUDING DEFAULTS) ON COMMIT DROP",
            conn, tx))
            await create.ExecuteNonQueryAsync(ct);

        await using (var importer = await conn.BeginBinaryImportAsync(
            "COPY temp_ticks (source, seq, ticker, ts, price, volume) FROM STDIN (FORMAT BINARY)", ct))
        {
            foreach (var t in batch)
            {
                await importer.StartRowAsync(ct);
                await importer.WriteAsync(t.SourceId, NpgsqlDbType.Text, ct);
                await importer.WriteAsync(t.SourceSeq, NpgsqlDbType.Bigint, ct);
                await importer.WriteAsync(t.Ticker, NpgsqlDbType.Text, ct);
                await importer.WriteAsync(t.TimestampUtc, NpgsqlDbType.TimestampTz, ct);
                await importer.WriteAsync(t.Price, NpgsqlDbType.Numeric, ct);
                await importer.WriteAsync(t.Volume, NpgsqlDbType.Numeric, ct);
            }
            await importer.CompleteAsync(ct);
        }

        // explicit column list: SELECT * would break on the first ALTER TABLE ADD COLUMN
        await using var insert = new NpgsqlCommand(
            "INSERT INTO ticks (source, seq, ticker, ts, price, volume) "
            + "SELECT source, seq, ticker, ts, price, volume FROM temp_ticks ON CONFLICT DO NOTHING", conn, tx);
        var inserted = await insert.ExecuteNonQueryAsync(ct);

        await tx.CommitAsync(ct);
        return inserted;
    }

    private static bool IsTransient(Exception ex) => ex switch
    {
        NpgsqlException { IsTransient: true } => true,
        TimeoutException => true,
        SocketException => true,
        _ => false
    };
}
