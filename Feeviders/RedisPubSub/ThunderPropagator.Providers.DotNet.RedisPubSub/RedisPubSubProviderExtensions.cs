using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.RedisPubSub
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
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}
