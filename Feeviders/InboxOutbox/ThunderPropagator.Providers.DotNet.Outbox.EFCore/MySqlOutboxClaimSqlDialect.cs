namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// MySQL/MariaDB (via the official <c>MySql.EntityFrameworkCore</c> provider): <c>FOR UPDATE SKIP LOCKED</c>,
    /// supported since MySQL 8.0 (InnoDB only - not MyISAM). The SQL text itself is identical regardless
    /// of which .NET client library connects - this is server-side syntax, not provider-specific.
    /// </summary>
    internal sealed class MySqlOutboxClaimSqlDialect : IOutboxClaimSqlDialect
    {
        private const string SqlTemplate =
            """
            SELECT `Id` FROM {TABLE}
            WHERE ((`PartitionKey` IS NULL AND {0} IS NULL) OR `PartitionKey` = {0})
              AND (
                `Status` = 'Pending'
                OR (`Status` = 'Publishing' AND (`LeaseExpiresAtUtc` IS NULL OR `LeaseExpiresAtUtc` <= {1}))
                OR (`Status` = 'Failed' AND (`NextRetryAtUtc` IS NULL OR `NextRetryAtUtc` <= {1}))
              )
            ORDER BY `OrderingSequence`
            LIMIT {2}
            FOR UPDATE SKIP LOCKED
            """;

        public string BuildClaimCandidateIdsSql(string quotedTable) => SqlTemplate.Replace("{TABLE}", quotedTable);
    }
}
