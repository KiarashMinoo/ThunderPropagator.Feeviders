using Microsoft.Extensions.DependencyInjection;

namespace ThunderPropagator.Providers.DotNet.Outbox.InMemory
{
    public static class InMemoryOutboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="InMemoryOutboxStore"/> as the Outbox backend resolvable via
        /// <see cref="IOutboxStoreFactory.GetStore"/> under <see cref="OutboxStoreType.InMemory"/> - a
        /// thin convenience over <see cref="OutboxStoreServiceCollectionExtensions.AddOutboxStore"/> for
        /// callers who do not need any custom construction. See <see cref="InMemoryOutboxStore"/> for its
        /// non-durability, process-local, and non-enlistable limitations.
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Providers reference via <see cref="OutboxOptions.StoreConnectionName"/> to resolve this backend.</param>
        /// <param name="timeProvider">
        /// Clock the store stamps timestamps and lease/retry expiry with. Defaults to
        /// <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic lease-expiry
        /// and backoff assertions.
        /// </param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddInMemoryOutboxStore(this IServiceCollection services, string storeName, TimeProvider? timeProvider = null) =>
            services.AddOutboxStore(storeName, OutboxStoreType.InMemory, _ => new InMemoryOutboxStore(timeProvider));
    }
}
