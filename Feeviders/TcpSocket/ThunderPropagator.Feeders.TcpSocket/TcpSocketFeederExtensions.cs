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

namespace ThunderPropagator.Feeders.TcpSocket
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
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IServiceCollection AddTcpSocketFeederResolver<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TTcpSocketFeederMessage : TcpSocketFeederMessage
            where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>,
                TTcpSocketFeederMessage,
                TTcpSocketFeederConfiguration>(services, (serviceProvider, channel, tcpSocketFeederConfiguration, feederHandler) =>
                new TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>(channel, tcpSocketFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

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