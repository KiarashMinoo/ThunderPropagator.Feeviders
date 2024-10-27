using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RapidStreamer.Providers.DotNet.SharedKernel.Extensions;

namespace RapidStreamer.Providers.DotNet.RedisPubSub
{
    public static class RedisPubSubProviderExtensions
    {
        public static IServiceCollection AddRedisPubSubProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration>(this IServiceCollection services,
            IConfigurationRoot configuration,
            string sectionName)
            where TRedisPubSubProviderMessage : RedisPubSubProviderMessage
            where TRedisPubSubProviderConfiguration : RedisPubSubProviderConfiguration, new()
        {
            TRedisPubSubProviderConfiguration redisPubSubProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(redisPubSubProviderConfiguration);
            services.TryAddSingleton(redisPubSubProviderConfiguration);

            services.AddChannelProvider<RedisPubSubProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration>, TRedisPubSubProviderMessage,
                TRedisPubSubProviderConfiguration>();

            return services;
        }
    }
}