using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Infrastructure.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Feeders.UdpClient
{
    public static class UdpClientFeederExtensions
    {
        public static IServiceCollection AddUdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TUdpClientFeederMessage : UdpClientFeederMessage
            where TUdpClientFeederConfiguration : UdpClientFeederConfiguration, new()
        {
            TUdpClientFeederConfiguration udpClientFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(udpClientFeederConfiguration);
            services.TryAddSingleton(udpClientFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>,
                TUdpClientFeederMessage,
                TUdpClientFeederConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IServiceCollection AddUdpClientFeederResolver<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TUdpClientFeederMessage : UdpClientFeederMessage
            where TUdpClientFeederConfiguration : UdpClientFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>,
                TUdpClientFeederMessage,
                TUdpClientFeederConfiguration>(services, (serviceProvider, channel, udpClientFeederConfiguration, feederHandler) =>
                new UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>(channel, udpClientFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IApplicationBuilder UseUdpClientFeederResolver<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TUdpClientFeederConfiguration udpClientFeederConfiguration)
            where TChannel : class, IChannel
            where TUdpClientFeederMessage : UdpClientFeederMessage
            where TUdpClientFeederConfiguration : UdpClientFeederConfiguration
        {
            var udpClientFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>>();

            udpClientFeederManager.UseFeeder(channelKey, udpClientFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}