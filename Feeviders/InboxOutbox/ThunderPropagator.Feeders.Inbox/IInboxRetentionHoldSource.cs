namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Optional legal/operational hold hook consulted by <see cref="InboxPurgeWorker"/> before every
    /// purge batch. A caller supplies an implementation only if it has such a requirement (e.g. an
    /// entry under active investigation, or subject to a legal retention hold) - no implementation is
    /// registered by default, and none is required.
    /// </summary>
    public interface IInboxRetentionHoldSource
    {
        /// <summary>
        /// IDs of entries currently on hold for <paramref name="channelKey"/> - never purged regardless
        /// of age, by passing them as <see cref="InboxPurgeRequest.ExcludedIds"/>. An empty set means
        /// nothing is currently held.
        /// </summary>
        Task<IReadOnlySet<Guid>> GetHeldEntryIdsAsync(Guid channelKey, CancellationToken cancellationToken);
    }
}
