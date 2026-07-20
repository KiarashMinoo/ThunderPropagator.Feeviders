using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.RedisPubSub
{
    internal static class RedisPubSubProviderTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.redispubsub");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.redispubsub");

        internal static readonly Counter<long> MessagesPublished =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.published");

        internal static readonly Counter<long> MessagesPublishFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.publish.failed");

        internal static readonly Histogram<double> PublishDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.redispubsub.publish.duration", unit: "ms");
    }

    public static class RedisPubSubProviderExtensions
    {
        public static IServiceCollection AddRedisPubSubProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration>(this IServiceCollection services,
            IConfigurationRoot configuration,
            string sectionName)
            where TRedisPubSubProviderMessage : RedisPubSubProviderMessage
            where TRedisPubSubProviderConfiguration : RedisPubSubProviderConfiguration, new()
        {
            TRedisPubSubProviderConfiguration redisPubSubProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(redisPubSubProviderConfiguration);
            services.TryAddSingleton(redisPubSubProviderConfiguration);

            services.AddChannelProvider<RedisPubSubProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration>, TRedisPubSubProviderMessage,
                TRedisPubSubProviderConfiguration>();

            return services;
        }
    }
}