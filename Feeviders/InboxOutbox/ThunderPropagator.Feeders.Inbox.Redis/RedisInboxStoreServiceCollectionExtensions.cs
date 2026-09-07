using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace ThunderPropagator.Feeders.Inbox.Redis
{
    public static class RedisInboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="RedisInboxStore"/> as the Inbox backend resolvable via
        /// <see cref="IInboxStoreFactory.GetStore"/> under <see cref="InboxStoreType.Redis"/> - a thin
        /// convenience over <see cref="InboxStoreServiceCollectionExtensions.AddInboxStore"/>. Uses
        /// <paramref name="storeName"/> itself as the store's Redis key-namespace prefix (see
        /// <see cref="RedisInboxStore"/>), so two stores registered under different names never collide
        /// in the same Redis database.
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Feeviders reference via <see cref="InboxOptions.StoreConnectionName"/> to resolve this backend. Also used as the Redis key-namespace prefix.</param>
        /// <param name="resolveConnection">Resolves the shared <see cref="IConnectionMultiplexer"/> this store uses - typically an already-registered singleton connection.</param>
        /// <param name="timeProvider">Clock the store stamps timestamps and lease/retry expiry with. Defaults to <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic assertions.</param>
        /// <param name="terminalEntryTtl">See <see cref="RedisInboxStore"/>'s remarks on passive retention. <see langword="null"/> disables it.</param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddRedisInboxStore(
            this IServiceCollection services,
            string storeName,
            Func<IServiceProvider, IConnectionMultiplexer> resolveConnection,
            TimeProvider? timeProvider = null,
            TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(resolveConnection);

            return services.AddInboxStore(storeName, InboxStoreType.Redis,
                sp => new RedisInboxStore(resolveConnection(sp), storeName, timeProvider, terminalEntryTtl));
        }
    }
}
