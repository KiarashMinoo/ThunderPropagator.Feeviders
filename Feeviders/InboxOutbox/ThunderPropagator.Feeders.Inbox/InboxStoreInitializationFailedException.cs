namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Thrown by <c>InboxRetryWorker.StartAsync</c> when a subscription's store implements
    /// <see cref="IInboxStoreInitializer"/> and <see cref="IInboxStoreInitializer.InitializeAsync"/>
    /// returns a <see cref="StoreInitializationResult"/> where <see cref="StoreInitializationResult.IsReady"/>
    /// is <see langword="false"/> - the worker never starts polling any subscription when this happens,
    /// even ones whose own store initialized fine, since a partially-started worker would be harder to
    /// reason about than one that never started at all.
    /// </summary>
    public sealed class InboxStoreInitializationFailedException : Exception
    {
        /// <summary>The channel whose store failed to initialize.</summary>
        public Guid ChannelKey { get; }

        /// <summary>The failing result - see <see cref="StoreInitializationResult.Outcome"/> for why.</summary>
        public StoreInitializationResult Result { get; }

        public InboxStoreInitializationFailedException(Guid channelKey, StoreInitializationResult result)
            : base($"Inbox store initialization for channel '{channelKey}' did not succeed: {result.Outcome} - {result.Message}", result.Error)
        {
            ChannelKey = channelKey;
            Result = result;
        }
    }
}
