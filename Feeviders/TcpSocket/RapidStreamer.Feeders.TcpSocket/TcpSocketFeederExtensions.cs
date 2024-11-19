using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Infrastructure.Extensions;

namespace RapidStreamer.Feeders.TcpSocket
{
    public static class TcpSocketFeederExtensions
    {
        public static IServiceCollection AddTcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TTcpSocketFeederMessage : TcpSocketFeederMessage
            where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration, new()
        {
            TTcpSocketFeederConfiguration tcpSocketFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(tcpSocketFeederConfiguration);
            services.TryAddSingleton(tcpSocketFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>,
                TTcpSocketFeederMessage,
                TTcpSocketFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddTcpSocketFeederResolver<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TTcpSocketFeederMessage : TcpSocketFeederMessage
            where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration, new()
        {
            services.AddChannelFeederResolver<TChannel,
                TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>,
                TTcpSocketFeederMessage,
                TTcpSocketFeederConfiguration>((serviceProvider, channel, tcpSocketFeederConfiguration, feederHandler) =>
                new TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>(channel, tcpSocketFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IApplicationBuilder UseTcpSocketFeederResolver<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TTcpSocketFeederConfiguration tcpSocketFeederConfiguration)
            where TChannel : class, IChannel
            where TTcpSocketFeederMessage : TcpSocketFeederMessage
            where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration
        {
            var tcpSocketFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>>();

            tcpSocketFeederManager.UseFeeder(channelKey, tcpSocketFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}