namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// One named <see cref="IInboxStore"/> backend, registered via
    /// <see cref="InboxStoreServiceCollectionExtensions.AddInboxStore"/>. Multiple Feeviders may
    /// reference the same <see cref="StoreName"/> - <see cref="IInboxStoreFactory"/> resolves it to a
    /// single shared instance, which is safe because dedup/claim keys are already scoped by
    /// <see cref="InboxClaimRequest.ChannelKey"/>/<see cref="InboxClaimRequest.FeederId"/>, so sharing
    /// one backend never collides two Feeviders' entries.
    /// </summary>
    public sealed record InboxStoreRegistration
    {
        /// <summary>The name Feeviders reference via <see cref="InboxOptions.StoreConnectionName"/> to resolve this backend.</summary>
        public required string StoreName { get; init; }

        /// <summary>
        /// The backend kind this registration provides. <see cref="IInboxStoreFactory.GetStore"/> checks
        /// this against the caller's expectation, so a Feevider configured for one backend can never be
        /// silently handed a different one under the same name.
        /// </summary>
        public required InboxStoreType StoreType { get; init; }

        /// <summary>
        /// Builds the store instance. Invoked at most once per <see cref="StoreName"/> - see
        /// <see cref="IInboxStoreFactory"/> - so this may safely open a connection or other owned
        /// resource rather than only returning an already-open one.
        /// </summary>
        public required Func<IServiceProvider, IInboxStore> CreateStore { get; init; }
    }
}
