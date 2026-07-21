using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.UdpClient
{
    public static class UdpClientProviderExtensions
    {
        public static IServiceCollection AddUdpClientProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TUdpClientProviderMessage : UdpClientProviderMessage
            where TUdpClientProviderConfiguration : UdpClientProviderConfiguration, new()
        {
            TUdpClientProviderConfiguration udpClientProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(udpClientProviderConfiguration);
            services.TryAddSingleton(udpClientProviderConfiguration);

            services.AddChannelProvider<UdpClientProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>, TUdpClientProviderMessage, TUdpClientProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}