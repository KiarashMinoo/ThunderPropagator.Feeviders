using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.Pulsar
{
    public static class PulsarProviderExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.pulsar");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.pulsar");
        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>("thunderpropagator.feeviders.pulsar.messages.published");
        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>("thunderpropagator.feeviders.pulsar.messages.publish.failed");
        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>("thunderpropagator.feeviders.pulsar.publish.duration", unit: "ms");

        public static IServiceCollection AddPulsarProvider<TPulsarProviderMessage, TPulsarProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TPulsarProviderMessage : PulsarProviderMessage
            where TPulsarProviderConfiguration : PulsarProviderConfiguration, new()
        {
            TPulsarProviderConfiguration pulsarProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(pulsarProviderConfiguration);
            services.TryAddSingleton(pulsarProviderConfiguration);

            services.AddChannelProvider<PulsarProvider<TPulsarProviderMessage, TPulsarProviderConfiguration>, TPulsarProviderMessage, TPulsarProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}