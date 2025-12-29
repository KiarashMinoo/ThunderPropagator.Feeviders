using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.NATS
{
    public static class NatsFeederExtensions
    {
        public static IServiceCollection AddNatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TNatsFeederMessage : NatsFeederMessage
            where TNatsFeederConfiguration : NatsFeederConfiguration, new()
        {
            TNatsFeederConfiguration NatsFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(NatsFeederConfiguration);
            services.TryAddSingleton(NatsFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                NatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>,
                TNatsFeederMessage,
                TNatsFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddNatsFeederResolver<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TNatsFeederMessage : NatsFeederMessage
            where TNatsFeederConfiguration : NatsFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                NatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>,
                TNatsFeederMessage,
                TNatsFeederConfiguration>(services, (serviceProvider, channel, natsFeederConfiguration, feederHandler) =>
                new NatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>(channel, natsFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IApplicationBuilder UseNatsFeederResolver<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TNatsFeederConfiguration natsFeederConfiguration)
            where TChannel : class, IChannel
            where TNatsFeederMessage : NatsFeederMessage
            where TNatsFeederConfiguration : NatsFeederConfiguration
        {
            var NatsFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>>();

            NatsFeederManager.UseFeeder(channelKey, natsFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}