using Npgsql;

namespace StockAggregator.Storage;

// Applies schema on startup with retries (DB may still be booting in compose)
// and signals readiness — writers wait for it instead of failing first batches.
public sealed class DatabaseInitializer(string connectionString, ILogger<DatabaseInitializer> logger)
    : BackgroundService
{
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Ready => _ready.Task;

    private const string Migration = """
        CREATE TABLE IF NOT EXISTS schema_migrations(
            version int primary key,
            applied_at timestamptz not null default now());

        CREATE TABLE IF NOT EXISTS ticks(
            source  text        not null,
            seq     bigint      not null,
            ticker  text        not null,
            ts      timestamptz not null,
            price   numeric     not null,
            volume  numeric     not null,
            primary key (source, seq));

        CREATE INDEX IF NOT EXISTS ix_ticks_ticker_ts ON ticks(ticker, ts);

        INSERT INTO schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING;
        """;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var conn = new NpgsqlConnection(connectionString);
                await conn.OpenAsync(stoppingToken);
                await using var cmd = new NpgsqlCommand(Migration, conn);
                await cmd.ExecuteNonQueryAsync(stoppingToken);

                logger.LogInformation("database schema is ready");
                _ready.TrySetResult();
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "database not ready, retrying in 2s");
                await Task.Delay(2000, stoppingToken);
            }
        }
    }
}
