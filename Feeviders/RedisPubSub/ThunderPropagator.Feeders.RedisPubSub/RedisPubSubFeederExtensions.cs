using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.RedisPubSub
{
    internal static class RedisPubSubFeederTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.redispubsub");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.redispubsub");

        internal static readonly Counter<long> MessagesReceived =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.received");

        internal static readonly Counter<long> MessagesReceiveFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.receive.failed");

        internal static readonly Histogram<double> ReceiveDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.redispubsub.receive.duration", unit: "ms");
    }

    public static class RedisPubSubFeederExtensions
    {
        public static IServiceCollection AddRedisPubSubFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TRedisPubSubFeederMessage : RedisPubSubFeederMessage
            where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration, new()
        {
            TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(redisPubSubFeederConfiguration);
            services.TryAddSingleton(redisPubSubFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                RedisPubSubFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>,
                TRedisPubSubFeederMessage,
                TRedisPubSubFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddRedisPubSubFeederResolver<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TRedisPubSubFeederMessage : RedisPubSubFeederMessage, new()
            where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                RedisPubSubFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>,
                TRedisPubSubFeederMessage,
                TRedisPubSubFeederConfiguration>(services, (serviceProvider, channel, redisPubSubFeederConfiguration, feederHandler) =>
                new RedisPubSubFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>(channel, redisPubSubFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IApplicationBuilder UseRedisPubSubFeederResolver<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration)
            where TChannel : class, IChannel
            where TRedisPubSubFeederMessage : RedisPubSubFeederMessage
            where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration
        {
            var redisPubSubFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>>();

            redisPubSubFeederManager.UseFeeder(channelKey, redisPubSubFeederConfiguration,
                app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}