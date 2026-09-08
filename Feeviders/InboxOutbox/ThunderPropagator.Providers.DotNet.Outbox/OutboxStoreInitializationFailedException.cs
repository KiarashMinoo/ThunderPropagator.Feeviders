namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Thrown by <c>OutboxRelayWorker.StartAsync</c> when a subscription's store implements
    /// <see cref="IOutboxStoreInitializer"/> and <see cref="IOutboxStoreInitializer.InitializeAsync"/>
    /// returns a <see cref="StoreInitializationResult"/> where <see cref="StoreInitializationResult.IsReady"/>
    /// is <see langword="false"/> - the worker never starts relaying any subscription when this happens,
    /// even ones whose own store initialized fine, since a partially-started worker would be harder to
    /// reason about than one that never started at all.
    /// </summary>
    public sealed class OutboxStoreInitializationFailedException : Exception
    {
        /// <summary>The provider whose store failed to initialize.</summary>
        public string ProviderKey { get; }

        /// <summary>The failing result - see <see cref="StoreInitializationResult.Outcome"/> for why.</summary>
        public StoreInitializationResult Result { get; }

        public OutboxStoreInitializationFailedException(string providerKey, StoreInitializationResult result)
            : base($"Outbox store initialization for provider '{providerKey}' did not succeed: {result.Outcome} - {result.Message}", result.Error)
        {
            ProviderKey = providerKey;
            Result = result;
        }
    }
}
