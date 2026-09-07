using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ThunderPropagator.Feeders.Inbox
{
    public static class InboxRetryServiceCollectionExtensions
    {
        /// <summary>
        /// Registers one channel's <see cref="InboxRetrySubscription"/> and ensures
        /// <see cref="InboxRetryWorker"/> itself is registered (singleton) so it aggregates every
        /// subscription added this way via <see cref="IEnumerable{T}"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException"><paramref name="channelKey"/> was already registered.</exception>
        public static IServiceCollection AddInboxRetrySubscription(
            this IServiceCollection services,
            Guid channelKey,
            InboxOptions options,
            Func<IServiceProvider, IInboxRetryHandler> createHandler,
            Func<IServiceProvider, IInboxDeadLetterHandler>? createDeadLetterHandler = null)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(createHandler);

            foreach (var descriptor in services)
            {
                if (descriptor.ServiceType == typeof(InboxRetrySubscription)
                    && descriptor.ImplementationInstance is InboxRetrySubscription { } existing
                    && existing.ChannelKey == channelKey)
                    throw new InvalidOperationException($"Channel '{channelKey}' already has an Inbox retry subscription.");
            }

            services.AddSingleton(new InboxRetrySubscription
            {
                ChannelKey = channelKey,
                Options = options,
                CreateHandler = createHandler,
                CreateDeadLetterHandler = createDeadLetterHandler,
            });

            services.TryAddSingleton<InboxRetryWorker>();

            return services;
        }
    }
}
