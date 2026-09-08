namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// One Provider <see cref="OutboxPurgeWorker"/> purges terminal entries for - bundles the same
    /// <see cref="OutboxOptions"/> the Provider was constructed with (so retention/batch parameters and
    /// the store reference stay in lockstep with the live enqueue and relay paths) with an optional
    /// legal/operational hold hook. Unlike <c>OutboxRelaySubscription</c>, this has no dependency on
    /// <c>IProvider</c> - purging only ever touches <see cref="IOutboxStore"/> - so it lives in this core
    /// project rather than the Providers SharedKernel.
    /// </summary>
    public sealed record OutboxPurgeSubscription
    {
        /// <summary>The Provider this subscription purges for - matches <see cref="OutboxMessage.ProviderKey"/>.</summary>
        public required string ProviderKey { get; init; }

        /// <summary>
        /// The same options the Provider was constructed with. Ignored (the subscription is skipped
        /// entirely) when <see cref="OutboxOptions.OutboxEnabled"/> is <see langword="false"/>.
        /// </summary>
        public required OutboxOptions Options { get; init; }

        /// <summary>
        /// Builds this Provider's legal/operational hold source, consulted before every purge batch.
        /// <see langword="null"/> (default) means nothing is ever held.
        /// </summary>
        public Func<IServiceProvider, IOutboxRetentionHoldSource>? CreateHoldSource { get; init; }
    }
}
