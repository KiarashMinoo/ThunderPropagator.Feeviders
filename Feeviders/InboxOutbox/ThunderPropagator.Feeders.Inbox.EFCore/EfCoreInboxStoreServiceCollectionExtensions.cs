using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace ThunderPropagator.Feeders.Inbox
{
    public static class EfCoreInboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="EfCoreInboxStore"/> as the Inbox backend resolvable via
        /// <see cref="IInboxStoreFactory.GetStore"/> under <see cref="InboxStoreType.EFCore"/> - a thin
        /// convenience over <see cref="InboxStoreServiceCollectionExtensions.AddInboxStore"/>.
        /// </summary>
        /// <param name="services">The service collection to register into.</param>
        /// <param name="storeName">The name Feeviders reference via <see cref="InboxOptions.StoreConnectionName"/> to resolve this backend.</param>
        /// <param name="createDbContext">Resolves a new, unshared <see cref="DbContext"/> per operation - see <see cref="EfCoreInboxStore"/>. Typically wraps an <see cref="IDbContextFactory{TContext}"/> already registered by the caller.</param>
        /// <param name="timeProvider">Clock the store stamps timestamps and lease/retry expiry with. Defaults to <see cref="TimeProvider.System"/>; tests should supply a fake for deterministic assertions.</param>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddEfCoreInboxStore(
            this IServiceCollection services,
            string storeName,
            Func<IServiceProvider, DbContext> createDbContext,
            TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(createDbContext);

            return services.AddInboxStore(storeName, InboxStoreType.EFCore,
                sp => new EfCoreInboxStore(() => createDbContext(sp), timeProvider));
        }
    }
}
