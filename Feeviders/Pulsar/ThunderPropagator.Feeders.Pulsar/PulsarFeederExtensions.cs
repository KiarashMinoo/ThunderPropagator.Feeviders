using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.Pulsar.SharedKernel;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.Pulsar
{
    public static class PulsarFeederExtensions
    {
        public static IServiceCollection AddPulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TPulsarFeederMessage : PulsarFeederMessage
            where TPulsarFeederConfiguration : PulsarFeederConfiguration, new()
        {
            TPulsarFeederConfiguration pulsarFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(pulsarFeederConfiguration);
            services.TryAddSingleton(pulsarFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                PulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>,
                TPulsarFeederMessage,
                TPulsarFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddPulsarFeederResolver<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TPulsarFeederMessage : PulsarFeederMessage
            where TPulsarFeederConfiguration : PulsarFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                PulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>,
                TPulsarFeederMessage,
                TPulsarFeederConfiguration>(services, (serviceProvider, channel, pulsarFeederConfiguration, feederHandler) =>
                new PulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>(channel, pulsarFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IApplicationBuilder UsePulsarFeederResolver<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TPulsarFeederConfiguration pulsarFeederConfiguration)
            where TChannel : class, IChannel
            where TPulsarFeederMessage : PulsarFeederMessage
            where TPulsarFeederConfiguration : PulsarFeederConfiguration
        {
            var pulsarFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>>();

            pulsarFeederManager.UseFeeder(channelKey, pulsarFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}