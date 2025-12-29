using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.WebSocket
{
    public static class WebSocketProviderExtensions
    {
        public static IServiceCollection AddWebSocketProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration>(this IServiceCollection services,
            IConfigurationRoot configuration,
            string sectionName)
            where TWebSocketProviderMessage : WebSocketProviderMessage
            where TWebSocketProviderConfiguration : WebSocketProviderConfiguration, new()
        {
            TWebSocketProviderConfiguration webSocketProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(webSocketProviderConfiguration);
            services.TryAddSingleton(webSocketProviderConfiguration);

            services.AddChannelProvider<WebSocketProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration>, TWebSocketProviderMessage, TWebSocketProviderConfiguration>();

            return services;
        }
    }
}