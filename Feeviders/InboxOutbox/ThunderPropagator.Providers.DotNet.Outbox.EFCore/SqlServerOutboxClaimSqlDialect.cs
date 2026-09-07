namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// SQL Server: <c>WITH (UPDLOCK, ROWLOCK, READPAST)</c> - <c>READPAST</c> is SQL Server's skip-locked
    /// equivalent (there is no <c>SKIP LOCKED</c> keyword); <c>UPDLOCK</c> takes an update lock instead of
    /// a shared one so the immediately-following <c>UPDATE</c> never has to escalate it; <c>ROWLOCK</c>
    /// keeps locking at row granularity rather than letting the engine escalate to a page/table lock
    /// under load.
    /// </summary>
    internal sealed class SqlServerOutboxClaimSqlDialect : IOutboxClaimSqlDialect
    {
        private const string SqlTemplate =
            """
            SELECT TOP ({2}) [Id] FROM {TABLE} WITH (UPDLOCK, ROWLOCK, READPAST)
            WHERE (([PartitionKey] IS NULL AND {0} IS NULL) OR [PartitionKey] = {0})
              AND (
                [Status] = 'Pending'
                OR ([Status] = 'Publishing' AND ([LeaseExpiresAtUtc] IS NULL OR [LeaseExpiresAtUtc] <= {1}))
                OR ([Status] = 'Failed' AND ([NextRetryAtUtc] IS NULL OR [NextRetryAtUtc] <= {1}))
              )
            ORDER BY [OrderingSequence]
            """;

        public string BuildClaimCandidateIdsSql(string quotedTable) => SqlTemplate.Replace("{TABLE}", quotedTable);
    }
}
