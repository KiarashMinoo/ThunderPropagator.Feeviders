namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Provider-specific SQL for <see cref="EfCoreOutboxStore.ClaimBatchAsync"/>'s locking read: select
    /// the ids of every currently-claimable row in one partition, in
    /// <see cref="OutboxMessage.OrderingSequence"/> order, taking a row lock on each and skipping any row
    /// already locked by a concurrent claim - a "skip locked" scan, not a blocking one, so independent
    /// relay workers claiming different rows of the same partition never queue up behind each other.
    /// </summary>
    /// <remarks>
    /// Deliberately returns only <see cref="OutboxMessage.Id"/> values (via a raw
    /// <c>DbContext.Database.SqlQueryRaw&lt;Guid&gt;</c> call), not full entities - EF Core's SQL query
    /// mapping for a full entity type requires the SQL to project every mapped column, which would
    /// couple this SQL to <see cref="OutboxMessageEntityTypeConfiguration"/>'s exact column set. The
    /// caller re-selects the locked ids as tracked <see cref="OutboxMessage"/> entities through a normal
    /// LINQ query afterward, in the same transaction/connection - since that connection already holds
    /// the lock this query took, that second read sees the rows without contending with itself.
    /// </remarks>
    internal interface IOutboxClaimSqlDialect
    {
        /// <param name="quotedTable">The schema-qualified, provider-quoted table name (e.g. <c>"schema"."table"</c> or <c>[schema].[table]</c>) to select from.</param>
        /// <returns>
        /// Parameterized SQL using positional placeholders <c>{0}</c> (partition key, possibly null),
        /// <c>{1}</c> (the current instant, as a <see cref="DateTimeOffset"/>), and <c>{2}</c> (max row
        /// count) - the shape <c>DbContext.Database.SqlQueryRaw</c> expects.
        /// </returns>
        string BuildClaimCandidateIdsSql(string quotedTable);
    }
}
