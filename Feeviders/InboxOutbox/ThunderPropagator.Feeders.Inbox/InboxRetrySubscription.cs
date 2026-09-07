namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// One channel <see cref="InboxRetryWorker"/> polls for retry-eligible entries - bundles the same
    /// <see cref="InboxOptions"/> the channel's own Feevider validated at startup (so backoff/lease/batch
    /// parameters and the store reference stay in lockstep with the live receive path) with the handler
    /// that reprocesses a reclaimed entry.
    /// </summary>
    public sealed record InboxRetrySubscription
    {
        /// <summary>The channel this subscription polls - matches <see cref="InboxMessage.ChannelKey"/>.</summary>
        public required Guid ChannelKey { get; init; }

        /// <summary>
        /// The same options the channel's Feevider was constructed with. Ignored (the subscription is
        /// skipped entirely) when <see cref="InboxOptions.InboxEnabled"/> is <see langword="false"/>.
        /// </summary>
        public required InboxOptions Options { get; init; }

        /// <summary>Builds the handler that reprocesses a reclaimed entry for this channel.</summary>
        public required Func<IServiceProvider, IInboxRetryHandler> CreateHandler { get; init; }

        /// <summary>
        /// Optionally builds a handler notified after an entry from this channel is dead-lettered. When
        /// <see langword="null"/>, dead-lettering proceeds with no notification - the extension point a
        /// future dead-letter pipeline plugs into is simply absent until one is registered.
        /// </summary>
        public Func<IServiceProvider, IInboxDeadLetterHandler>? CreateDeadLetterHandler { get; init; }
    }
}
