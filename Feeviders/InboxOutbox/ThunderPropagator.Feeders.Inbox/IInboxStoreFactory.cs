namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Resolves the shared <see cref="IInboxStore"/> behind a named <see cref="InboxStoreRegistration"/>.
    /// Every implementation must construct at most one instance per store name and hand out the same
    /// instance on every subsequent call, regardless of how many Feeviders or messages ask for it - a
    /// caller should resolve once (typically when a Feevider starts) and reuse the result, never call
    /// this per message.
    /// </summary>
    public interface IInboxStoreFactory
    {
        /// <summary>
        /// Resolves the store registered as <paramref name="storeName"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// No store is registered under <paramref name="storeName"/>, or it was registered as a
        /// different <see cref="InboxStoreType"/> than <paramref name="expectedStoreType"/> - both fail
        /// synchronously, before any message consumption that depends on the result begins.
        /// </exception>
        IInboxStore GetStore(string storeName, InboxStoreType expectedStoreType);
    }
}
