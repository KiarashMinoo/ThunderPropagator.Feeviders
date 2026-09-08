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
        /// Builds every handler run, in order, by <see cref="InboxDeadLetterPipeline"/> after an entry
        /// from this channel is dead-lettered. Empty by default - dead-lettering proceeds with no
        /// notification when no handlers are registered.
        /// </summary>
        public IReadOnlyList<Func<IServiceProvider, IInboxDeadLetterHandler>> CreateDeadLetterHandlers { get; init; } = [];

        /// <summary>Payload visibility policy for the <see cref="InboxDeadLetterContext"/> handed to <see cref="CreateDeadLetterHandlers"/>. Defaults to <see cref="InboxDeadLetterPayloadPolicy.Include"/>.</summary>
        public InboxDeadLetterPayloadPolicy DeadLetterPayloadPolicy { get; init; } = InboxDeadLetterPayloadPolicy.Include;
    }
}
