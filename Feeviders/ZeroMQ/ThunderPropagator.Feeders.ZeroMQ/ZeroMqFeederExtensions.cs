using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Infrastructure.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Feeders.ZeroMQ
{
    public static class ZeroMqFeederExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.zeromq");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.zeromq");
        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>("thunderpropagator.feeviders.zeromq.messages.received");
        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>("thunderpropagator.feeviders.zeromq.messages.receive.failed");
        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>("thunderpropagator.feeviders.zeromq.receive.duration", unit: "ms");

        public static IServiceCollection AddZeroMqFeeder<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TZeroMqFeederMessage : ZeroMqFeederMessage
            where TZeroMqFeederConfiguration : ZeroMqFeederConfiguration, new()
        {
            TZeroMqFeederConfiguration zeroMqFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(zeroMqFeederConfiguration);
            services.TryAddSingleton(zeroMqFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                ZeroMqFeeder<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>,
                TZeroMqFeederMessage,
                TZeroMqFeederConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IServiceCollection AddZeroMqFeederResolver<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TZeroMqFeederMessage : ZeroMqFeederMessage
            where TZeroMqFeederConfiguration : ZeroMqFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                ZeroMqFeeder<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>,
                TZeroMqFeederMessage,
                TZeroMqFeederConfiguration>(services, (serviceProvider, channel, zeroMqFeederConfiguration, feederHandler) =>
                new ZeroMqFeeder<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>(channel, zeroMqFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IApplicationBuilder UseZeroMqFeederResolver<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TZeroMqFeederConfiguration zeroMqFeederConfiguration)
            where TChannel : class, IChannel
            where TZeroMqFeederMessage : ZeroMqFeederMessage
            where TZeroMqFeederConfiguration : ZeroMqFeederConfiguration
        {
            var zeroMqFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>>();

            zeroMqFeederManager.UseFeeder(channelKey, zeroMqFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}
