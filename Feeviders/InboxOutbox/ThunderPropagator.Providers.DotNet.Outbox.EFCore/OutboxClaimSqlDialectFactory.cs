using Microsoft.EntityFrameworkCore;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Resolves the <see cref="IOutboxClaimSqlDialect"/> matching a <see cref="DbContext"/>'s active
    /// provider (<c>Database.ProviderName</c>) - the "provider capability abstraction" a
    /// caller's choice of <c>UseSqlServer</c>/<c>UseNpgsql</c>/<c>UseMySQL</c> selects into. This package
    /// never references those provider packages itself (only <c>Microsoft.EntityFrameworkCore.Relational</c>) -
    /// a caller pulls in whichever one they actually use, and this factory only needs to recognize its
    /// provider name string at runtime, never link against its assembly.
    /// </summary>
    /// <remarks>
    /// MySQL is supported via Oracle's own official <c>MySql.EntityFrameworkCore</c> provider - there is
    /// no Microsoft-published MySQL provider (SQL Server is Microsoft's own database; PostgreSQL/MySQL
    /// are both third-party), and this deliberately does not use the community
    /// <c>Pomelo.EntityFrameworkCore.MySql</c> provider instead.
    /// </remarks>
    internal static class OutboxClaimSqlDialectFactory
    {
        private const string SqlServerProviderName = "Microsoft.EntityFrameworkCore.SqlServer";
        private const string NpgsqlProviderName = "Npgsql.EntityFrameworkCore.PostgreSQL";
        private const string MySqlProviderName = "MySql.EntityFrameworkCore";

        public static IOutboxClaimSqlDialect Resolve(DbContext dbContext)
        {
            var providerName = dbContext.Database.ProviderName;

            return providerName switch
            {
                SqlServerProviderName => new SqlServerOutboxClaimSqlDialect(),
                NpgsqlProviderName => new PostgreSqlOutboxClaimSqlDialect(),
                MySqlProviderName => new MySqlOutboxClaimSqlDialect(),
                _ => throw new NotSupportedException(
                    $"EfCoreOutboxStore has no SKIP LOCKED claim dialect for provider '{providerName}'. " +
                    $"Supported providers: {SqlServerProviderName}, {NpgsqlProviderName}, {MySqlProviderName}."),
            };
        }

        /// <summary>The schema-qualified, provider-quoted table name for <see cref="OutboxMessage"/> in <paramref name="dbContext"/>'s model - see <see cref="IOutboxClaimSqlDialect.BuildClaimCandidateIdsSql"/>.</summary>
        public static string GetQuotedTableName(DbContext dbContext)
        {
            var entityType = dbContext.Model.FindEntityType(typeof(OutboxMessage))
                ?? throw new InvalidOperationException(
                    $"'{typeof(OutboxMessage)}' is not part of this DbContext's model - apply {nameof(OutboxMessageEntityTypeConfiguration)} in OnModelCreating first.");

            var table = entityType.GetTableName()
                ?? throw new InvalidOperationException($"'{typeof(OutboxMessage)}' has no mapped table name.");
            var schema = entityType.GetSchema();

            var (open, close) = dbContext.Database.ProviderName switch
            {
                SqlServerProviderName => ('[', ']'),
                MySqlProviderName => ('`', '`'),
                _ => ('"', '"'), // PostgreSQL, and a reasonable ANSI-SQL default otherwise.
            };

            return schema is null
                ? $"{open}{table}{close}"
                : $"{open}{schema}{close}.{open}{table}{close}";
        }
    }
}
