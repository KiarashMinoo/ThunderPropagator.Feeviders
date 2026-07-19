using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.AwsSqs
{
    public static class SnsProviderExtensions
    {
        public static IServiceCollection AddSnsProvider<TSnsProviderMessage, TSnsProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TSnsProviderMessage : SnsProviderMessage
            where TSnsProviderConfiguration : SnsProviderConfiguration, new()
        {
            TSnsProviderConfiguration snsProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(snsProviderConfiguration);
            services.TryAddSingleton(snsProviderConfiguration);

            services.AddChannelProvider<SnsProvider<TSnsProviderMessage, TSnsProviderConfiguration>, TSnsProviderMessage, TSnsProviderConfiguration>();

            return services;
        }
    }
}
