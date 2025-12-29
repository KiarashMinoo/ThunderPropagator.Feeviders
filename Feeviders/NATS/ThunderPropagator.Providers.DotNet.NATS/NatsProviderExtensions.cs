using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.NATS
{
    public static class NatsProviderExtensions
    {
        public static IServiceCollection AddNatsProvider<TNatsProviderMessage, TNatsProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TNatsProviderMessage : NatsProviderMessage
            where TNatsProviderConfiguration : NatsProviderConfiguration, new()
        {
            TNatsProviderConfiguration natsProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(natsProviderConfiguration);
            services.TryAddSingleton(natsProviderConfiguration);

            services.AddChannelProvider<NatsProvider<TNatsProviderMessage, TNatsProviderConfiguration>, TNatsProviderMessage, TNatsProviderConfiguration>();

            return services;
        }
    }
}