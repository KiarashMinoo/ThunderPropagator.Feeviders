using Microsoft.EntityFrameworkCore;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Acquires/releases a session-scoped advisory lock, keyed by an arbitrary string name, on whichever
    /// provider a given <see cref="DbContext"/>'s <c>Database.ProviderName</c> identifies - the "provider
    /// lock" <see cref="EfCoreOutboxStore.InitializeAsync"/> needs to serialize concurrent replicas'
    /// schema creation/upgrade attempts. Session-scoped uniformly across all three providers, since
    /// MySQL's <c>GET_LOCK</c>/<c>RELEASE_LOCK</c> has no transaction-scoped equivalent at all - the
    /// caller must keep the same open connection alive (<c>Database.OpenConnectionAsync</c>) for the
    /// whole acquire-work-release sequence and always call <see cref="ReleaseAsync"/>, even on failure.
    /// </summary>
    /// <remarks>
    /// PostgreSQL uses <c>pg_try_advisory_lock</c> (immediate, non-blocking) in a caller-driven retry
    /// loop; SQL Server's <c>sp_getapplock</c> and MySQL's <c>GET_LOCK</c> both support a native blocking
    /// wait with a timeout directly, so no retry loop is needed for either. Only the PostgreSQL path is
    /// exercised against a real database in this repo's own tests - see the #127 commit message for the
    /// documented gap on the other two.
    /// </remarks>
    /// <remarks>
    /// Every scalar query below aliases its projected column as <c>Value</c> - EF Core's scalar
    /// <c>Database.SqlQueryRaw&lt;TResult&gt;</c> projects the result through a column literally named
    /// that, and omitting the alias fails with a "column ... does not exist" error at the SQL level,
    /// confirmed empirically against PostgreSQL.
    /// </remarks>
    internal static class SchemaLockDialect
    {
        private const string SqlServerProviderName = "Microsoft.EntityFrameworkCore.SqlServer";
        private const string NpgsqlProviderName = "Npgsql.EntityFrameworkCore.PostgreSQL";
        private const string MySqlProviderName = "MySql.EntityFrameworkCore";

        private static readonly TimeSpan PostgresPollInterval = TimeSpan.FromMilliseconds(200);

        public static Task<bool> TryAcquireAsync(DbContext db, string lockName, TimeSpan timeout, CancellationToken cancellationToken) =>
            db.Database.ProviderName switch
            {
                NpgsqlProviderName => TryAcquirePostgresAsync(db, lockName, timeout, cancellationToken),
                SqlServerProviderName => TryAcquireSqlServerAsync(db, lockName, timeout, cancellationToken),
                MySqlProviderName => TryAcquireMySqlAsync(db, lockName, timeout, cancellationToken),
                var providerName => throw new NotSupportedException($"No schema initialization lock dialect for provider '{providerName}'."),
            };

        public static Task ReleaseAsync(DbContext db, string lockName, CancellationToken cancellationToken) =>
            db.Database.ProviderName switch
            {
                NpgsqlProviderName => db.Database.ExecuteSqlRawAsync("SELECT pg_advisory_unlock(hashtext({0})::bigint)", [lockName], cancellationToken),
                SqlServerProviderName => db.Database.ExecuteSqlRawAsync("EXEC sp_releaseapplock @Resource = {0}, @LockOwner = 'Session'", [lockName], cancellationToken),
                MySqlProviderName => db.Database.ExecuteSqlRawAsync("SELECT RELEASE_LOCK({0})", [lockName], cancellationToken),
                _ => Task.CompletedTask,
            };

        private static async Task<bool> TryAcquirePostgresAsync(DbContext db, string lockName, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var deadline = DateTime.UtcNow + timeout;
            do
            {
                var acquired = await db.Database.SqlQueryRaw<bool>("SELECT pg_try_advisory_lock(hashtext({0})::bigint) AS \"Value\"", lockName)
                    .SingleAsync(cancellationToken).ConfigureAwait(false);
                if (acquired)
                    return true;

                await Task.Delay(PostgresPollInterval, cancellationToken).ConfigureAwait(false);
            } while (DateTime.UtcNow < deadline);

            return false;
        }

        private static async Task<bool> TryAcquireSqlServerAsync(DbContext db, string lockName, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var result = await db.Database.SqlQueryRaw<int>(
                    "DECLARE @tp_lock_result int; EXEC @tp_lock_result = sp_getapplock @Resource = {0}, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = {1}; SELECT @tp_lock_result AS [Value];",
                    lockName, (int)timeout.TotalMilliseconds)
                .SingleAsync(cancellationToken).ConfigureAwait(false);

            return result >= 0;
        }

        private static async Task<bool> TryAcquireMySqlAsync(DbContext db, string lockName, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var result = await db.Database.SqlQueryRaw<int?>("SELECT GET_LOCK({0}, {1}) AS `Value`", lockName, (int)timeout.TotalSeconds)
                .SingleAsync(cancellationToken).ConfigureAwait(false);

            return result == 1;
        }
    }
}
