using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.RabbitMQ
{
    public static class RabbitMQFeederExtensions
    {
        public static IServiceCollection AddRabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TRabbitMQFeederMessage : RabbitMQFeederMessage
            where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration, new()
        {
            TRabbitMQFeederConfiguration rabbitMqFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(rabbitMqFeederConfiguration);
            services.TryAddSingleton(rabbitMqFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                RabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>,
                TRabbitMQFeederMessage,
                TRabbitMQFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddRabbitMQFeederResolver<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TRabbitMQFeederMessage : RabbitMQFeederMessage
            where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                RabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>,
                TRabbitMQFeederMessage,
                TRabbitMQFeederConfiguration>(services, (serviceProvider, channel, rabbitMqFeederConfiguration, feederHandler) =>
                new RabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>(channel, rabbitMqFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IApplicationBuilder UseRabbitMQFeederResolver<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TRabbitMQFeederConfiguration rabbitMQFeederConfiguration)
            where TChannel : class, IChannel
            where TRabbitMQFeederMessage : RabbitMQFeederMessage
            where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration
        {
            var rabbitMQFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>>();

            rabbitMQFeederManager.UseFeeder(channelKey, rabbitMQFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}