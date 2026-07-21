using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.Mqtt
{
    public static class MqttProviderExtensions
    {
        public static IServiceCollection AddMqttProvider<TMqttProviderMessage, TMqttProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TMqttProviderMessage : MqttProviderMessage
            where TMqttProviderConfiguration : MqttProviderConfiguration, new()
        {
            TMqttProviderConfiguration mqttProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(mqttProviderConfiguration);
            services.TryAddSingleton(mqttProviderConfiguration);

            services.AddChannelProvider<MqttProvider<TMqttProviderMessage, TMqttProviderConfiguration>, TMqttProviderMessage, TMqttProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}
