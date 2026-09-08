using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    public static class OutboxRelayServiceCollectionExtensions
    {
        /// <summary>
        /// Registers one Provider's <see cref="OutboxRelaySubscription"/> and ensures
        /// <see cref="OutboxRelayWorker"/> itself is registered (singleton) so it aggregates every
        /// subscription added this way via <see cref="IEnumerable{T}"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException"><paramref name="providerKey"/> was already registered.</exception>
        public static IServiceCollection AddOutboxRelaySubscription(
            this IServiceCollection services,
            string providerKey,
            OutboxOptions options,
            Func<IServiceProvider, IProvider> resolveProvider,
            IReadOnlyList<Func<IServiceProvider, IOutboxDeadLetterHandler>>? createDeadLetterHandlers = null,
            OutboxDeadLetterPayloadPolicy deadLetterPayloadPolicy = OutboxDeadLetterPayloadPolicy.Include)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(providerKey);
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(resolveProvider);

            foreach (var descriptor in services)
            {
                if (descriptor.ServiceType == typeof(OutboxRelaySubscription)
                    && descriptor.ImplementationInstance is OutboxRelaySubscription { } existing
                    && existing.ProviderKey == providerKey)
                    throw new InvalidOperationException($"Provider '{providerKey}' already has an Outbox relay subscription.");
            }

            services.AddSingleton(new OutboxRelaySubscription
            {
                ProviderKey = providerKey,
                Options = options,
                ResolveProvider = resolveProvider,
                CreateDeadLetterHandlers = createDeadLetterHandlers ?? [],
                DeadLetterPayloadPolicy = deadLetterPayloadPolicy,
            });

            services.TryAddSingleton<OutboxRelayWorker>();

            return services;
        }
    }
}
