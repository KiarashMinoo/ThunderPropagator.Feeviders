using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.TcpSocket
{
    public static class TcpSocketProviderExtensions
    {
        public static IServiceCollection AddTcpSocketProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TTcpSocketProviderMessage : TcpSocketProviderMessage
            where TTcpSocketProviderConfiguration : TcpSocketProviderConfiguration, new()
        {
            TTcpSocketProviderConfiguration tcpSocketProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(tcpSocketProviderConfiguration);
            services.TryAddSingleton(tcpSocketProviderConfiguration);

            services.AddChannelProvider<TcpSocketProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>, TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>();

            return services;
        }
    }
}