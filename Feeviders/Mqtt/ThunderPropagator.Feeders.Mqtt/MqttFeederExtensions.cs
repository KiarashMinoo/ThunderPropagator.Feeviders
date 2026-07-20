using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.Mqtt
{
    internal static class MqttFeederTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.mqtt");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.mqtt");

        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.received",
            description: "Number of MQTT messages successfully received.");

        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.receive.failed",
            description: "Number of MQTT messages that failed to be received/processed.");

        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.mqtt.receive.duration",
            unit: "ms",
            description: "Duration of MQTT message receive processing.");
    }

    public static class MqttFeederExtensions
    {
        public static IServiceCollection AddMqttFeeder<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TMqttFeederMessage : MqttFeederMessage, new()
            where TMqttFeederConfiguration : MqttFeederConfiguration, new()
        {
            TMqttFeederConfiguration mqttFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(mqttFeederConfiguration);
            services.TryAddSingleton(mqttFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                MqttFeeder<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>,
                TMqttFeederMessage,
                TMqttFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddMqttFeederResolver<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TMqttFeederMessage : MqttFeederMessage, new()
            where TMqttFeederConfiguration : MqttFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                MqttFeeder<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>,
                TMqttFeederMessage,
                TMqttFeederConfiguration>(services, (serviceProvider, channel, mqttFeederConfiguration, feederHandler) =>
                new MqttFeeder<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>(channel, mqttFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IApplicationBuilder UseMqttFeederResolver<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TMqttFeederConfiguration mqttFeederConfiguration)
            where TChannel : class, IChannel
            where TMqttFeederMessage : MqttFeederMessage
            where TMqttFeederConfiguration : MqttFeederConfiguration
        {
            var mqttFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>>();

            mqttFeederManager.UseFeeder(channelKey, mqttFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}