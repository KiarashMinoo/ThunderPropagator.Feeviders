using Microsoft.Extensions.DependencyInjection;

namespace ThunderPropagator.Feeders.Inbox.InMemory
{
    public static class InMemoryInboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="InMemoryInboxStore"/> as the Inbox backend resolvable via
        /// <see cref="IInboxStoreFactory.GetStore"/> under <see cref="InboxStoreType.InMemory"/> - a thin
        /// convenience over <see cref="InboxStoreServiceCollectionExtensions.AddInboxStore"/> for callers
        /// who do not need any custom construction. See <see cref="InMemoryInboxStore"/> for its
        /// non-durability and process-local limitations.
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Feeviders reference via <see cref="InboxOptions.StoreConnectionName"/> to resolve this backend.</param>
        /// <param name="timeProvider">
        /// Clock the store stamps timestamps and lease/retry expiry with. Defaults to
        /// <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic lease-expiry
        /// and backoff assertions.
        /// </param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddInMemoryInboxStore(this IServiceCollection services, string storeName, TimeProvider? timeProvider = null) =>
            services.AddInboxStore(storeName, InboxStoreType.InMemory, _ => new InMemoryInboxStore(timeProvider));
    }
}
