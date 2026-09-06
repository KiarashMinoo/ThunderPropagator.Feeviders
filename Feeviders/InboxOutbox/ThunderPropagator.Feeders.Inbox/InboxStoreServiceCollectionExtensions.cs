using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ThunderPropagator.Feeders.Inbox
{
    public static class InboxStoreServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named <see cref="IInboxStore"/> backend, resolvable via
        /// <see cref="IInboxStoreFactory.GetStore"/>, and ensures <see cref="IInboxStoreFactory"/>
        /// itself is registered (as <see cref="InboxStoreFactory"/>, singleton). Call once per distinct
        /// <paramref name="storeName"/> - a second call with the same name throws immediately, rather
        /// than waiting for the ambiguity to surface at resolution time.
        /// </summary>
        /// <exception cref="ArgumentException"><paramref name="storeName"/> is empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException"><paramref name="storeName"/> was already registered.</exception>
        public static IServiceCollection AddInboxStore(
            this IServiceCollection services,
            string storeName,
            InboxStoreType storeType,
            Func<IServiceProvider, IInboxStore> createStore)
        {
            if (string.IsNullOrWhiteSpace(storeName))
                throw new ArgumentException("Store name must not be empty.", nameof(storeName));

            ArgumentNullException.ThrowIfNull(createStore);

            foreach (var descriptor in services)
            {
                if (descriptor.ServiceType == typeof(InboxStoreRegistration)
                    && descriptor.ImplementationInstance is InboxStoreRegistration { } existing
                    && existing.StoreName == storeName)
                    throw new InvalidOperationException($"An Inbox store named '{storeName}' is already registered.");
            }

            services.AddSingleton(new InboxStoreRegistration
            {
                StoreName = storeName,
                StoreType = storeType,
                CreateStore = createStore,
            });

            services.TryAddSingleton<IInboxStoreFactory, InboxStoreFactory>();

            return services;
        }
    }
}
