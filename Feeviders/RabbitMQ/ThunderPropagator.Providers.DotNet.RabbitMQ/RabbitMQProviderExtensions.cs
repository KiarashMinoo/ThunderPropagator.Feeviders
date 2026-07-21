using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OpenTelemetry.Context.Propagation;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.RabbitMQ
{
    public static class RabbitMQProviderExtensions
    {
        internal static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.rabbitmq");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.rabbitmq");

        internal static readonly Counter<long> MessagesPublished =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.rabbitmq.messages.published");
        internal static readonly Counter<long> MessagesPublishFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.rabbitmq.messages.publish.failed");
        internal static readonly Histogram<double> PublishDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.rabbitmq.publish.duration", unit: "ms");

        public static IServiceCollection AddRabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TRabbitMQProviderMessage : RabbitMQProviderMessage
            where TRabbitMQProviderConfiguration : RabbitMQProviderConfiguration, new()
        {
            TRabbitMQProviderConfiguration rabbitMQProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(rabbitMQProviderConfiguration);
            services.TryAddSingleton(rabbitMQProviderConfiguration);

            services.AddChannelProvider<RabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>, TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}