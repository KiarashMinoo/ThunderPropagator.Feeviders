using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    public static class OutboxPurgeServiceCollectionExtensions
    {
        /// <summary>
        /// Registers one Provider's <see cref="OutboxPurgeSubscription"/> and ensures
        /// <see cref="OutboxPurgeWorker"/> itself is registered (singleton) so it aggregates every
        /// subscription added this way via <see cref="IEnumerable{T}"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException"><paramref name="providerKey"/> was already registered.</exception>
        public static IServiceCollection AddOutboxPurgeSubscription(
            this IServiceCollection services,
            string providerKey,
            OutboxOptions options,
            Func<IServiceProvider, IOutboxRetentionHoldSource>? createHoldSource = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(providerKey);
            ArgumentNullException.ThrowIfNull(options);

            foreach (var descriptor in services)
            {
                if (descriptor.ServiceType == typeof(OutboxPurgeSubscription)
                    && descriptor.ImplementationInstance is OutboxPurgeSubscription { } existing
                    && existing.ProviderKey == providerKey)
                    throw new InvalidOperationException($"Provider '{providerKey}' already has an Outbox purge subscription.");
            }

            services.AddSingleton(new OutboxPurgeSubscription
            {
                ProviderKey = providerKey,
                Options = options,
                CreateHoldSource = createHoldSource,
            });

            services.TryAddSingleton<OutboxPurgeWorker>();

            return services;
        }
    }
}
