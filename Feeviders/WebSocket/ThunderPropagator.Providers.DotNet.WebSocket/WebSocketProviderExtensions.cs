using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.WebSocket
{
    public static class WebSocketProviderExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.websocket");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.websocket");

        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.websocket.messages.published",
            description: "Number of WebSocket messages successfully published.");

        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.websocket.messages.publish.failed",
            description: "Number of WebSocket messages that failed to be published.");

        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.websocket.publish.duration",
            unit: "ms",
            description: "Duration of WebSocket message publish operations.");

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