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