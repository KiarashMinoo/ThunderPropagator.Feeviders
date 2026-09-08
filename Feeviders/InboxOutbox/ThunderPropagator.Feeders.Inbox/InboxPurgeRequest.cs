namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Bounds and filters one <see cref="IInboxStore.PurgeAsync"/> batch. Each terminal status's cutoff
    /// is independent and optional, so a caller can purge Processed and DeadLettered entries on
    /// different retention windows - or skip one status entirely (e.g. while DeadLettered entries await
    /// manual triage).
    /// </summary>
    public sealed record InboxPurgeRequest
    {
        /// <summary>The channel to purge - purging is always scoped to one channel, never global.</summary>
        public required Guid ChannelKey { get; init; }

        /// <summary>Purge Processed entries whose <see cref="InboxMessage.ProcessedAtUtc"/> is older than this. <see langword="null"/> skips Processed entirely.</summary>
        public DateTimeOffset? ProcessedOlderThanUtc { get; init; }

        /// <summary>Purge DeadLettered entries whose <see cref="InboxMessage.DeadLetteredAtUtc"/> is older than this. <see langword="null"/> skips DeadLettered entirely.</summary>
        public DateTimeOffset? DeadLetteredOlderThanUtc { get; init; }

        /// <summary>
        /// Upper bound on how many entries this one call deletes, so a purge run never monopolizes store
        /// locks/connections for an unbounded amount of time - see <see cref="InboxPurgeResult.HasMore"/>
        /// for continuing across multiple bounded calls. Bounded by
        /// <see cref="InboxMessageLimits.MaxPurgeBatchSize"/>.
        /// </summary>
        public int MaxCount { get; init; } = 500;

        /// <summary>
        /// Entry IDs that must never be purged by this call even if otherwise eligible - the
        /// legal/operational hold hook. <see langword="null"/> or empty holds nothing back.
        /// </summary>
        public IReadOnlySet<Guid>? ExcludedIds { get; init; }
    }
}
