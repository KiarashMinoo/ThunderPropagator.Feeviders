namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Resolves the shared <see cref="IOutboxStore"/> behind a named <see cref="OutboxStoreRegistration"/>.
    /// Every implementation must construct at most one instance per store name and hand out the same
    /// instance on every subsequent call, regardless of how many Providers or relay workers ask for it -
    /// a caller should resolve once (typically when a Provider or relay worker starts) and reuse the
    /// result, never call this per message.
    /// </summary>
    public interface IOutboxStoreFactory
    {
        /// <summary>
        /// Resolves the store registered as <paramref name="storeName"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// No store is registered under <paramref name="storeName"/>, or it was registered as a
        /// different <see cref="OutboxStoreType"/> than <paramref name="expectedStoreType"/> - both fail
        /// synchronously, before any publishing that depends on the result begins.
        /// </exception>
        IOutboxStore GetStore(string storeName, OutboxStoreType expectedStoreType);
    }
}
