using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ThunderPropagator.Feeders.Inbox
{
    public static class InboxPurgeServiceCollectionExtensions
    {
        /// <summary>
        /// Registers one channel's <see cref="InboxPurgeSubscription"/> and ensures
        /// <see cref="InboxPurgeWorker"/> itself is registered (singleton) so it aggregates every
        /// subscription added this way via <see cref="IEnumerable{T}"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException"><paramref name="channelKey"/> was already registered.</exception>
        public static IServiceCollection AddInboxPurgeSubscription(
            this IServiceCollection services,
            Guid channelKey,
            InboxOptions options,
            Func<IServiceProvider, IInboxRetentionHoldSource>? createHoldSource = null)
        {
            ArgumentNullException.ThrowIfNull(options);

            foreach (var descriptor in services)
            {
                if (descriptor.ServiceType == typeof(InboxPurgeSubscription)
                    && descriptor.ImplementationInstance is InboxPurgeSubscription { } existing
                    && existing.ChannelKey == channelKey)
                    throw new InvalidOperationException($"Channel '{channelKey}' already has an Inbox purge subscription.");
            }

            services.AddSingleton(new InboxPurgeSubscription
            {
                ChannelKey = channelKey,
                Options = options,
                CreateHoldSource = createHoldSource,
            });

            services.TryAddSingleton<InboxPurgeWorker>();

            return services;
        }
    }
}
