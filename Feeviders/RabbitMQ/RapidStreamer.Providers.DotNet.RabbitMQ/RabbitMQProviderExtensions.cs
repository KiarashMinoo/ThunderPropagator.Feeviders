using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OpenTelemetry.Context.Propagation;
using RapidStreamer.Providers.DotNet.SharedKernel.Extensions;

namespace RapidStreamer.Providers.DotNet.RabbitMQ
{
    public static class RabbitMQProviderExtensions
    {
        internal static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

        public static IServiceCollection AddRabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TRabbitMQProviderMessage : RabbitMQProviderMessage
            where TRabbitMQProviderConfiguration : RabbitMQProviderConfiguration, new()
        {
            TRabbitMQProviderConfiguration rabbitMQProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(rabbitMQProviderConfiguration);
            services.TryAddSingleton(rabbitMQProviderConfiguration);

            services.AddChannelProvider<RabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>, TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>();

            return services;
        }
    }
}