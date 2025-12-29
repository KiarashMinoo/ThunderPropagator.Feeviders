using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.Kafka
{
    public static class KafkaProviderExtensions
    {
        public static IServiceCollection AddKafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TKafkaProviderMessage : KafkaProviderMessage
            where TKafkaProviderConfiguration : KafkaProviderConfiguration, new()
        {
            TKafkaProviderConfiguration kafkaProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(kafkaProviderConfiguration);
            services.TryAddSingleton(kafkaProviderConfiguration);

            services.AddChannelProvider<KafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>, TKafkaProviderMessage, TKafkaProviderConfiguration>();

            return services;
        }
    }
}