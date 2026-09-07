using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace ThunderPropagator.Providers.DotNet.Outbox.Redis
{
    public static class RedisOutboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="RedisOutboxStore"/> as the Outbox backend resolvable via
        /// <see cref="IOutboxStoreFactory.GetStore"/> under <see cref="OutboxStoreType.Redis"/> - a thin
        /// convenience over <see cref="OutboxStoreServiceCollectionExtensions.AddOutboxStore"/>. Uses
        /// <paramref name="storeName"/> itself as the store's Redis key-namespace prefix (see
        /// <see cref="RedisOutboxStore"/>), so two stores registered under different names never collide
        /// in the same Redis database.
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Providers reference via <see cref="OutboxOptions.StoreConnectionName"/> to resolve this backend. Also used as the Redis key-namespace prefix.</param>
        /// <param name="resolveConnection">Resolves the shared <see cref="IConnectionMultiplexer"/> this store uses - typically an already-registered singleton connection.</param>
        /// <param name="timeProvider">Clock the store stamps timestamps and lease/retry expiry with. Defaults to <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic assertions.</param>
        /// <param name="terminalEntryTtl">See <see cref="RedisOutboxStore"/>'s remarks on passive retention. <see langword="null"/> disables it.</param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddRedisOutboxStore(
            this IServiceCollection services,
            string storeName,
            Func<IServiceProvider, IConnectionMultiplexer> resolveConnection,
            TimeProvider? timeProvider = null,
            TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(resolveConnection);

            return services.AddOutboxStore(storeName, OutboxStoreType.Redis,
                sp => new RedisOutboxStore(resolveConnection(sp), storeName, timeProvider, terminalEntryTtl));
        }
    }
}
