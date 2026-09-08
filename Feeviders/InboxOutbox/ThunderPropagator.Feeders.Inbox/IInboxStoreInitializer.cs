namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Optional capability an <see cref="IInboxStore"/> backend implements when it has a persisted
    /// schema/index/key-layout that must exist (and match a known version) before the store is safe to
    /// use - see <see cref="InboxStoreType"/>'s remarks for which backends need this and why. A backend
    /// with nothing to persist (<c>InMemoryInboxStore</c>) implements it too, trivially, so callers never
    /// need to special-case "does this backend need initializing" - they just always call it.
    /// </summary>
    /// <remarks>
    /// Every implementation must be safe to call concurrently from multiple uncoordinated replicas
    /// racing the same underlying store at process startup - see each implementation's own remarks for
    /// how it serializes that race (a database advisory lock, an atomic compare-and-set document write,
    /// a Redis <c>SET ... NX</c>, or - for a backend with no shared state at all - nothing, because there
    /// is nothing to race over).
    /// </remarks>
    public interface IInboxStoreInitializer
    {
        /// <summary>
        /// Ensures this store's persisted schema exists and is compatible with what this code requires.
        /// Callers should call this once per store, before starting anything (an <c>InboxRetryWorker</c>,
        /// a live receive path) that calls <see cref="IInboxStore.TryClaimAsync"/> against it - see
        /// <see cref="StoreInitializationResult.IsReady"/>.
        /// </summary>
        Task<StoreInitializationResult> InitializeAsync(StoreInitializationMode mode = StoreInitializationMode.Apply, CancellationToken cancellationToken = default);
    }
}
