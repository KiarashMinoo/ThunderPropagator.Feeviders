using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace ThunderPropagator.Providers.DotNet.Outbox.MongoDB
{
    public static class MongoOutboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="MongoOutboxStore"/> as the Outbox backend resolvable via
        /// <see cref="IOutboxStoreFactory.GetStore"/> under <see cref="OutboxStoreType.MongoDB"/> - a thin
        /// convenience over <see cref="OutboxStoreServiceCollectionExtensions.AddOutboxStore"/>. Does not
        /// itself call <see cref="MongoOutboxStore.InitializeAsync"/> - run that once at startup
        /// separately (e.g. from a hosted service, or let <c>OutboxRelayWorker.StartAsync</c> do it).
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Providers reference via <see cref="OutboxOptions.StoreConnectionName"/> to resolve this backend.</param>
        /// <param name="resolveDatabase">Resolves the shared <see cref="IMongoDatabase"/> this store uses - typically an already-registered singleton.</param>
        /// <param name="collectionName">Message collection this store reads/writes. Defaults to <see cref="MongoOutboxStore.DefaultCollectionName"/>.</param>
        /// <param name="sequenceCollectionName">Ordering-sequence-counter collection. Defaults to <see cref="MongoOutboxStore.DefaultSequenceCollectionName"/>.</param>
        /// <param name="metaCollectionName">Schema-version metadata collection. Defaults to <see cref="MongoOutboxStore.DefaultMetaCollectionName"/>.</param>
        /// <param name="timeProvider">Clock the store stamps timestamps and lease/retry expiry with. Defaults to <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic assertions.</param>
        /// <param name="terminalEntryTtl">See <see cref="MongoOutboxStore"/>'s remarks on passive retention. <see langword="null"/> disables it.</param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddMongoOutboxStore(
            this IServiceCollection services,
            string storeName,
            Func<IServiceProvider, IMongoDatabase> resolveDatabase,
            string collectionName = MongoOutboxStore.DefaultCollectionName,
            string sequenceCollectionName = MongoOutboxStore.DefaultSequenceCollectionName,
            string metaCollectionName = MongoOutboxStore.DefaultMetaCollectionName,
            TimeProvider? timeProvider = null,
            TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(resolveDatabase);

            return services.AddOutboxStore(storeName, OutboxStoreType.MongoDB,
                sp => new MongoOutboxStore(resolveDatabase(sp), collectionName, sequenceCollectionName, metaCollectionName, timeProvider, terminalEntryTtl));
        }
    }
}
