namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Outcome of one <see cref="IOutboxStore.PurgeAsync"/> batch.</summary>
    public sealed record OutboxPurgeResult
    {
        /// <summary>How many entries this call actually deleted.</summary>
        public required int PurgedCount { get; init; }

        /// <summary>
        /// Whether more entries matching <see cref="OutboxPurgeRequest.PublishedOlderThanUtc"/>/
        /// <see cref="OutboxPurgeRequest.DeadLetteredOlderThanUtc"/> exist beyond this batch's
        /// <see cref="OutboxPurgeRequest.MaxCount"/> bound - independent of
        /// <see cref="OutboxPurgeRequest.ExcludedIds"/>, since a held entry still counts as "more work
        /// exists" even though this call will never delete it. A caller (e.g. <see cref="OutboxPurgeWorker"/>)
        /// should schedule another call while this is <see langword="true"/>.
        /// </summary>
        public required bool HasMore { get; init; }
    }
}
