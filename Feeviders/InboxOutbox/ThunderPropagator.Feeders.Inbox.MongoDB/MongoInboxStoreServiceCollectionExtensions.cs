using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace ThunderPropagator.Feeders.Inbox.MongoDB
{
    public static class MongoInboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="MongoInboxStore"/> as the Inbox backend resolvable via
        /// <see cref="IInboxStoreFactory.GetStore"/> under <see cref="InboxStoreType.MongoDB"/> - a thin
        /// convenience over <see cref="InboxStoreServiceCollectionExtensions.AddInboxStore"/>. Does not
        /// itself call <see cref="MongoInboxStore.InitializeAsync"/> - run that once at startup
        /// separately (e.g. from a hosted service, or let <c>InboxRetryWorker.StartAsync</c> do it), the
        /// same way an EF Core backend needs its own initializer run before first use.
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Feeviders reference via <see cref="InboxOptions.StoreConnectionName"/> to resolve this backend.</param>
        /// <param name="resolveDatabase">Resolves the shared <see cref="IMongoDatabase"/> this store uses - typically an already-registered singleton.</param>
        /// <param name="collectionName">Collection this store reads/writes. Defaults to <see cref="MongoInboxStore.DefaultCollectionName"/>; override to run more than one named store against the same database.</param>
        /// <param name="metaCollectionName">Schema-version metadata collection. Defaults to <see cref="MongoInboxStore.DefaultMetaCollectionName"/>.</param>
        /// <param name="timeProvider">Clock the store stamps timestamps and lease/retry expiry with. Defaults to <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic assertions.</param>
        /// <param name="terminalEntryTtl">See <see cref="MongoInboxStore"/>'s remarks on passive retention. <see langword="null"/> disables it.</param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddMongoInboxStore(
            this IServiceCollection services,
            string storeName,
            Func<IServiceProvider, IMongoDatabase> resolveDatabase,
            string collectionName = MongoInboxStore.DefaultCollectionName,
            string metaCollectionName = MongoInboxStore.DefaultMetaCollectionName,
            TimeProvider? timeProvider = null,
            TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(resolveDatabase);

            return services.AddInboxStore(storeName, InboxStoreType.MongoDB,
                sp => new MongoInboxStore(resolveDatabase(sp), collectionName, metaCollectionName, timeProvider, terminalEntryTtl));
        }
    }
}
