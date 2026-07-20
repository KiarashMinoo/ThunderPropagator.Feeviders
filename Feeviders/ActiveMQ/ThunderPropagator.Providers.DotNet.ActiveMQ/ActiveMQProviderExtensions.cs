using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.ActiveMQ
{
    internal static class ActiveMQProviderTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.activemq");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.activemq");

        internal static readonly Counter<long> MessagesPublished =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.activemq.messages.published");

        internal static readonly Counter<long> MessagesPublishFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.activemq.messages.publish.failed");

        internal static readonly Histogram<double> PublishDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.activemq.publish.duration", unit: "ms");
    }

    public static class ActiveMQProviderExtensions
    {
        public static IServiceCollection AddActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TActiveMQProviderMessage : ActiveMQProviderMessage
            where TActiveMQProviderConfiguration : ActiveMQProviderConfiguration, new()
        {
            TActiveMQProviderConfiguration activeMQProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(activeMQProviderConfiguration);
            services.TryAddSingleton(activeMQProviderConfiguration);

            services.AddChannelProvider<ActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>, TActiveMQProviderMessage, TActiveMQProviderConfiguration>();

            return services;
        }
    }
}