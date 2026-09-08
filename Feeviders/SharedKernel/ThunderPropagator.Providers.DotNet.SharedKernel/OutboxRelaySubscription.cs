using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    /// <summary>
    /// One Provider <see cref="OutboxRelayWorker"/> relays claimed entries for - bundles the same
    /// <see cref="OutboxOptions"/> the Provider's own <c>AbstractProvider</c> validated at construction
    /// (so backoff/lease/batch parameters and the store reference stay in lockstep with the live enqueue
    /// path) with however the worker resolves the <see cref="IProvider"/> instance to publish through.
    /// </summary>
    public sealed record OutboxRelaySubscription
    {
        /// <summary>
        /// The Provider this subscription relays for - matches <see cref="OutboxMessage.ProviderKey"/>
        /// (see <c>IAbstractProviderConfiguration.Id</c>).
        /// </summary>
        public required string ProviderKey { get; init; }

        /// <summary>
        /// The same options the Provider was constructed with. Ignored (the subscription is skipped
        /// entirely) when <see cref="OutboxOptions.OutboxEnabled"/> is <see langword="false"/>.
        /// </summary>
        public required OutboxOptions Options { get; init; }

        /// <summary>
        /// Resolves the <see cref="IProvider"/> instance to call <see cref="IProvider.PublishDirectAsync(byte[], IReadOnlyDictionary{string, string}?, CancellationToken)"/>
        /// on for every entry this subscription claims.
        /// </summary>
        public required Func<IServiceProvider, IProvider> ResolveProvider { get; init; }

        /// <summary>
        /// Builds every handler run, in order, by <see cref="OutboxDeadLetterPipeline"/> after an entry
        /// from this Provider is dead-lettered. Empty by default - dead-lettering proceeds with no
        /// notification when no handlers are registered.
        /// </summary>
        public IReadOnlyList<Func<IServiceProvider, IOutboxDeadLetterHandler>> CreateDeadLetterHandlers { get; init; } = [];

        /// <summary>Payload visibility policy for the <see cref="OutboxDeadLetterContext"/> handed to <see cref="CreateDeadLetterHandlers"/>. Defaults to <see cref="OutboxDeadLetterPayloadPolicy.Include"/>.</summary>
        public OutboxDeadLetterPayloadPolicy DeadLetterPayloadPolicy { get; init; } = OutboxDeadLetterPayloadPolicy.Include;
    }
}
