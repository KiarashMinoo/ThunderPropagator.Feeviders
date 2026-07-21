using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.ActiveMQ
{
    public static class ActiveMQProviderExtensions
    {
        public static IServiceCollection AddActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TActiveMQProviderMessage : ActiveMQProviderMessage
            where TActiveMQProviderConfiguration : ActiveMQProviderConfiguration, new()
        {
            TActiveMQProviderConfiguration activeMQProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(activeMQProviderConfiguration);
            services.TryAddSingleton(activeMQProviderConfiguration);

            services.AddChannelProvider<ActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>, TActiveMQProviderMessage, TActiveMQProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}
