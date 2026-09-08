namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Outcome of one <see cref="IInboxStore.PurgeAsync"/> batch.</summary>
    public sealed record InboxPurgeResult
    {
        /// <summary>How many entries this call actually deleted.</summary>
        public required int PurgedCount { get; init; }

        /// <summary>
        /// Whether more entries matching <see cref="InboxPurgeRequest.ProcessedOlderThanUtc"/>/
        /// <see cref="InboxPurgeRequest.DeadLetteredOlderThanUtc"/> exist beyond this batch's
        /// <see cref="InboxPurgeRequest.MaxCount"/> bound - independent of
        /// <see cref="InboxPurgeRequest.ExcludedIds"/>, since a held entry still counts as "more work
        /// exists" even though this call will never delete it. A caller (e.g. <see cref="InboxPurgeWorker"/>)
        /// should schedule another call while this is <see langword="true"/>.
        /// </summary>
        public required bool HasMore { get; init; }
    }
}
