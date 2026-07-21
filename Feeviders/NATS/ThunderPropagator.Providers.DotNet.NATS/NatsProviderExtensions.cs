using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.NATS
{
    public static class NatsProviderExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.nats");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.nats");

        internal static readonly Counter<long> MessagesPublished =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.nats.messages.published");

        internal static readonly Counter<long> MessagesPublishFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.nats.messages.publish.failed");

        internal static readonly Histogram<double> PublishDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.nats.publish.duration", unit: "ms");

        public static IServiceCollection AddNatsProvider<TNatsProviderMessage, TNatsProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TNatsProviderMessage : NatsProviderMessage
            where TNatsProviderConfiguration : NatsProviderConfiguration, new()
        {
            TNatsProviderConfiguration natsProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(natsProviderConfiguration);
            services.TryAddSingleton(natsProviderConfiguration);

            services.AddChannelProvider<NatsProvider<TNatsProviderMessage, TNatsProviderConfiguration>, TNatsProviderMessage, TNatsProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}