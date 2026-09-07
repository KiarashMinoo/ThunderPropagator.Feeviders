namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// One named <see cref="IOutboxStore"/> backend, registered via
    /// <see cref="OutboxStoreServiceCollectionExtensions.AddOutboxStore"/>. Multiple Providers may
    /// reference the same <see cref="StoreName"/> - <see cref="IOutboxStoreFactory"/> resolves it to a
    /// single shared instance, which is safe because claim/ordering keys are already scoped by
    /// <see cref="OutboxMessage.ProviderKey"/>/<see cref="OutboxMessage.PartitionKey"/> (see
    /// <see cref="IOutboxStore.ClaimBatchAsync"/>), so sharing one backend never lets two Providers'
    /// entries collide or interleave each other's ordering.
    /// </summary>
    public sealed record OutboxStoreRegistration
    {
        /// <summary>The name Providers reference via <see cref="OutboxOptions.StoreConnectionName"/> to resolve this backend.</summary>
        public required string StoreName { get; init; }

        /// <summary>
        /// The backend kind this registration provides. <see cref="IOutboxStoreFactory.GetStore"/> checks
        /// this against the caller's expectation, so a Provider configured for one backend can never be
        /// silently handed a different one under the same name.
        /// </summary>
        public required OutboxStoreType StoreType { get; init; }

        /// <summary>
        /// Builds the store instance. Invoked at most once per <see cref="StoreName"/> - see
        /// <see cref="IOutboxStoreFactory"/> - so this may safely open a connection or other owned
        /// resource rather than only returning an already-open one.
        /// </summary>
        public required Func<IServiceProvider, IOutboxStore> CreateStore { get; init; }
    }
}
