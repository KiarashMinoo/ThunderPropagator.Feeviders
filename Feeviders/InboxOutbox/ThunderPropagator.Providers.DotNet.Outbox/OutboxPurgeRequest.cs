namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Bounds and filters one <see cref="IOutboxStore.PurgeAsync"/> batch. Each terminal status's cutoff
    /// is independent and optional, so a caller can purge Published and DeadLettered entries on
    /// different retention windows - or skip one status entirely (e.g. while DeadLettered entries await
    /// manual triage). Unlike Outbox claiming, purging is never partition-scoped - it always considers
    /// every partition.
    /// </summary>
    public sealed record OutboxPurgeRequest
    {
        /// <summary>Purge Published entries whose <see cref="OutboxMessage.PublishedAtUtc"/> is older than this. <see langword="null"/> skips Published entirely.</summary>
        public DateTimeOffset? PublishedOlderThanUtc { get; init; }

        /// <summary>Purge DeadLettered entries whose <see cref="OutboxMessage.DeadLetteredAtUtc"/> is older than this. <see langword="null"/> skips DeadLettered entirely.</summary>
        public DateTimeOffset? DeadLetteredOlderThanUtc { get; init; }

        /// <summary>
        /// Upper bound on how many entries this one call deletes, so a purge run never monopolizes store
        /// locks/connections for an unbounded amount of time - see <see cref="OutboxPurgeResult.HasMore"/>
        /// for continuing across multiple bounded calls. Bounded by
        /// <see cref="OutboxMessageLimits.MaxPurgeBatchSize"/>.
        /// </summary>
        public int MaxCount { get; init; } = 500;

        /// <summary>
        /// Entry IDs that must never be purged by this call even if otherwise eligible - the
        /// legal/operational hold hook. <see langword="null"/> or empty holds nothing back.
        /// </summary>
        public IReadOnlySet<Guid>? ExcludedIds { get; init; }
    }
}
