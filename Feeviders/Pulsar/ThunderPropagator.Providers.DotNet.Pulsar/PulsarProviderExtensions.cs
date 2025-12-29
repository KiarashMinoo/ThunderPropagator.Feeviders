using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.Pulsar
{
    public static class PulsarProviderExtensions
    {
        public static IServiceCollection AddPulsarProvider<TPulsarProviderMessage, TPulsarProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TPulsarProviderMessage : PulsarProviderMessage
            where TPulsarProviderConfiguration : PulsarProviderConfiguration, new()
        {
            TPulsarProviderConfiguration pulsarProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(pulsarProviderConfiguration);
            services.TryAddSingleton(pulsarProviderConfiguration);

            services.AddChannelProvider<PulsarProvider<TPulsarProviderMessage, TPulsarProviderConfiguration>, TPulsarProviderMessage, TPulsarProviderConfiguration>();

            return services;
        }
    }
}