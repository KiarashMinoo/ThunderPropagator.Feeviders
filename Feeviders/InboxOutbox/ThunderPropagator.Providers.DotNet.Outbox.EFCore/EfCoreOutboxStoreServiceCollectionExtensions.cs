using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    public static class EfCoreOutboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="EfCoreOutboxStore"/> as the Outbox backend resolvable via
        /// <see cref="IOutboxStoreFactory.GetStore"/> under <see cref="OutboxStoreType.EFCore"/> - a thin
        /// convenience over <see cref="OutboxStoreServiceCollectionExtensions.AddOutboxStore"/>.
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Providers reference via <see cref="OutboxOptions.StoreConnectionName"/> to resolve this backend.</param>
        /// <param name="createDbContext">Resolves a new, unshared <see cref="DbContext"/> per operation - see <see cref="EfCoreOutboxStore"/>. Typically wraps an <see cref="IDbContextFactory{TContext}"/> already registered by the caller.</param>
        /// <param name="timeProvider">Clock the store stamps timestamps and lease/retry expiry with. Defaults to <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic assertions.</param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddEfCoreOutboxStore(
            this IServiceCollection services,
            string storeName,
            Func<IServiceProvider, DbContext> createDbContext,
            TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(createDbContext);

            return services.AddOutboxStore(storeName, OutboxStoreType.EFCore,
                sp => new EfCoreOutboxStore(() => createDbContext(sp), timeProvider));
        }
    }
}
