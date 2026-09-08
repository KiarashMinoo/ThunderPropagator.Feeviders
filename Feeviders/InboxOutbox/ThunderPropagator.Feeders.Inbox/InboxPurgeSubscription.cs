namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// One channel <see cref="InboxPurgeWorker"/> purges terminal entries from - bundles the same
    /// <see cref="InboxOptions"/> the channel's own Feevider validated at startup (so retention/batch
    /// parameters and the store reference stay in lockstep with the live receive and retry paths) with
    /// an optional legal/operational hold hook.
    /// </summary>
    public sealed record InboxPurgeSubscription
    {
        /// <summary>The channel this subscription purges - matches <see cref="InboxMessage.ChannelKey"/>.</summary>
        public required Guid ChannelKey { get; init; }

        /// <summary>
        /// The same options the channel's Feevider was constructed with. Ignored (the subscription is
        /// skipped entirely) when <see cref="InboxOptions.InboxEnabled"/> is <see langword="false"/>.
        /// </summary>
        public required InboxOptions Options { get; init; }

        /// <summary>
        /// Builds this channel's legal/operational hold source, consulted before every purge batch.
        /// <see langword="null"/> (default) means nothing is ever held.
        /// </summary>
        public Func<IServiceProvider, IInboxRetentionHoldSource>? CreateHoldSource { get; init; }
    }
}
