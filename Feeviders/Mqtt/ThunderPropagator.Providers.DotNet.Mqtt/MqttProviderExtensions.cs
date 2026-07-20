using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.Mqtt
{
    internal static class MqttProviderTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.mqtt");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.mqtt");

        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.published",
            description: "Number of MQTT messages successfully published.");

        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.publish.failed",
            description: "Number of MQTT messages that failed to be published.");

        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.mqtt.publish.duration",
            unit: "ms",
            description: "Duration of MQTT message publish operations.");
    }

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

            return services;
        }
    }
}