using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.ActiveMQ
{
    public static class ActiveMQFeederExtensions
    {
        public static IServiceCollection AddActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TActiveMQFeederMessage : ActiveMQFeederMessage
            where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration, new()
        {
            TActiveMQFeederConfiguration activeMQFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(activeMQFeederConfiguration);
            services.TryAddSingleton(activeMQFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                ActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>,
                TActiveMQFeederMessage,
                TActiveMQFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddActiveMQFeederResolver<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TActiveMQFeederMessage : ActiveMQFeederMessage
            where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                ActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>,
                TActiveMQFeederMessage,
                TActiveMQFeederConfiguration>(services, (serviceProvider, channel, activeMQFeederConfiguration, feederHandler) =>
                new ActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>(channel, activeMQFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IApplicationBuilder UseActiveMQFeederResolver<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TActiveMQFeederConfiguration activeMQFeederConfiguration)
            where TChannel : class, IChannel
            where TActiveMQFeederMessage : ActiveMQFeederMessage
            where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration
        {
            var activeMQFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>>();

            activeMQFeederManager.UseFeeder(channelKey, activeMQFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}