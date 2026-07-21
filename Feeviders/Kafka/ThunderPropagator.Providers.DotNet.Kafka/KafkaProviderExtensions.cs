using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.Kafka
{
    public static class KafkaProviderExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.kafka");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.kafka");

        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.kafka.messages.published", "{message}", "Total messages published to Kafka");
        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.kafka.messages.publish.failed", "{message}", "Total Kafka publish failures");
        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.kafka.publish.duration", "ms", "Kafka message publish latency");

        public static IServiceCollection AddKafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TKafkaProviderMessage : KafkaProviderMessage
            where TKafkaProviderConfiguration : KafkaProviderConfiguration, new()
        {
            TKafkaProviderConfiguration kafkaProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(kafkaProviderConfiguration);
            services.TryAddSingleton(kafkaProviderConfiguration);

            services.AddChannelProvider<KafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>, TKafkaProviderMessage, TKafkaProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}