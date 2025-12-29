using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;

namespace ThunderPropagator.Feeders.WebSocket
{
    internal
#if !DEBUG
        sealed
#endif
        class WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration> : DelegativeFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TWebSocketFeederMessage : WebSocketFeederMessage
        where TWebSocketFeederConfiguration : WebSocketFeederConfiguration, IAbstractFeederConfiguration
    {
        public WebSocketFeeder(TChannel channel,
            TWebSocketFeederConfiguration webSocketFeederConfiguration,
            IFeederHandler<TChannel, TWebSocketFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, webSocketFeederConfiguration, feederHandler, serviceProvider)
        {
            HealthName = $"feeder_{nameof(WebSocket)}_{webSocketFeederConfiguration.Path.Replace("/", "_")}";
            HealthTags = [.. HealthTags, nameof(WebSocket), webSocketFeederConfiguration.Path.Replace("/", "_")];
        }

        internal async ValueTask EnqueueAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            try
            {
                ReportHealth(HealthStatus.Healthy);

                var webSocketFeederMessage = Deserialize(bytes, cancellationToken) ??
                                             throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");
                var activityContext = webSocketFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                var baggage = webSocketFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                await ReceiveAsync(webSocketFeederMessage, activityContext, baggage, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                ReportHealth(HealthStatus.Unhealthy, exception);

                Logger.LogError(exception, "Error while enqueuing message");
            }
        }
    }
}