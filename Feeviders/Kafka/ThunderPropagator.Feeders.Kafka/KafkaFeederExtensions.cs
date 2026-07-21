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
using ThunderPropagator.Infrastructure.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Feeders.Kafka
{
    public static class KafkaFeederExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.kafka");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.kafka");

        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.kafka.messages.received", "{message}", "Total messages received from Kafka");
        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.kafka.messages.receive.failed", "{message}", "Total Kafka receive failures");
        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.kafka.receive.duration", "ms", "Kafka message receive latency");

        public static IServiceCollection AddKafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TKafkaFeederMessage : KafkaFeederMessage, new()
            where TKafkaFeederConfiguration : KafkaFeederConfiguration, new()
        {
            TKafkaFeederConfiguration kafkaFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(kafkaFeederConfiguration);
            services.TryAddSingleton(kafkaFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                KafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>,
                TKafkaFeederMessage,
                TKafkaFeederConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IServiceCollection AddKafkaFeederResolver<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TKafkaFeederMessage : KafkaFeederMessage, new()
            where TKafkaFeederConfiguration : KafkaFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                KafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>,
                TKafkaFeederMessage,
                TKafkaFeederConfiguration>(services, (serviceProvider, channel, kafkaFeederConfiguration, feederHandler) =>
                new KafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>(channel, kafkaFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IApplicationBuilder UseKafkaFeederResolver<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TKafkaFeederConfiguration kafkaFeederConfiguration)
            where TChannel : class, IChannel
            where TKafkaFeederMessage : KafkaFeederMessage
            where TKafkaFeederConfiguration : KafkaFeederConfiguration
        {
            var kafkaFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>>();

            kafkaFeederManager.UseFeeder(channelKey, kafkaFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}