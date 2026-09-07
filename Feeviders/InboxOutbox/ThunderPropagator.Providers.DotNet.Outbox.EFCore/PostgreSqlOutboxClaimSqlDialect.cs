namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>PostgreSQL: <c>FOR UPDATE SKIP LOCKED</c>, supported since PostgreSQL 9.5.</summary>
    internal sealed class PostgreSqlOutboxClaimSqlDialect : IOutboxClaimSqlDialect
    {
        // {0} is cast to ::text explicitly: left bare, Npgsql/Postgres cannot infer a type for it from
        // an "x IS NULL" comparison alone (there is no other untyped usage in this query fixing it) and
        // raises "42P08: could not determine data type of parameter" - the cast gives it one regardless
        // of which branch of the OR actually applies at runtime.
        private const string SqlTemplate =
            """
            SELECT "Id" FROM {TABLE}
            WHERE (("PartitionKey" IS NULL AND {0}::text IS NULL) OR "PartitionKey" = {0}::text)
              AND (
                "Status" = 'Pending'
                OR ("Status" = 'Publishing' AND ("LeaseExpiresAtUtc" IS NULL OR "LeaseExpiresAtUtc" <= {1}))
                OR ("Status" = 'Failed' AND ("NextRetryAtUtc" IS NULL OR "NextRetryAtUtc" <= {1}))
              )
            ORDER BY "OrderingSequence"
            LIMIT {2}
            FOR UPDATE SKIP LOCKED
            """;

        public string BuildClaimCandidateIdsSql(string quotedTable) => SqlTemplate.Replace("{TABLE}", quotedTable);
    }
}
