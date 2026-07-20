using System.Diagnostics;
using System.Diagnostics.Metrics;
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
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.rabbitmq");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.rabbitmq");

        internal static readonly Counter<long> MessagesReceived =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.rabbitmq.messages.received");
        internal static readonly Counter<long> MessagesReceiveFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.rabbitmq.messages.receive.failed");
        internal static readonly Histogram<double> ReceiveDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.rabbitmq.receive.duration", unit: "ms");

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