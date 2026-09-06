using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Features;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.Feeders.WebSocket
{
    internal
#if !DEBUG
        sealed
#endif
        partial class WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration> : DelegativeFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TWebSocketFeederMessage : WebSocketFeederMessage
        where TWebSocketFeederConfiguration : WebSocketFeederConfiguration, IAbstractFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4700, Level = LogLevel.Error, Message = "Error while enqueuing message")]
            public static partial void EnqueueError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4701, Level = LogLevel.Error, Message = "Inbox processing on Endpoint {Endpoint} ended as {Outcome}.")]
            public static partial void InboxProcessingFailed(ILogger logger, string endpoint, string outcome);
        }

        private readonly InboxReceiveCoordinator _inboxCoordinator;

        public WebSocketFeeder(TChannel channel,
            TWebSocketFeederConfiguration webSocketFeederConfiguration,
            IFeederHandler<TChannel, TWebSocketFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, webSocketFeederConfiguration, feederHandler, serviceProvider)
        {
            HealthName = $"feeder_{nameof(WebSocket)}_{webSocketFeederConfiguration.Path.Replace("/", "_")}";
            HealthTags = [.. HealthTags, nameof(WebSocket), webSocketFeederConfiguration.Path.Replace("/", "_")];
            _inboxCoordinator = new InboxReceiveCoordinator(
                webSocketFeederConfiguration.Inbox, ChannelKey, Id, serviceProvider.GetService<IInboxStoreFactory>());
        }

        internal async ValueTask EnqueueAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            var receiveTimestamp = Stopwatch.GetTimestamp();
            Activity? activity = null;

            try
            {
                ReportHealth(HealthStatus.Healthy);

                var webSocketFeederMessage = Deserialize(bytes, cancellationToken) ??
                                             throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");
                var activityContextEntry = webSocketFeederMessage[nameof(ActivityContext)];
                var activityContext = activityContextEntry is ActivityContext ac ? ac : default;
                var baggage = webSocketFeederMessage[nameof(Baggage)] is Baggage b ? b : default;

                activity = activityContextEntry is ActivityContext
                    ? WebSocketFeederExtensions.ActivitySource.StartActivity("websocket receive", ActivityKind.Consumer, activityContext)
                    : WebSocketFeederExtensions.ActivitySource.StartActivity("websocket receive", ActivityKind.Consumer);
                activity?.SetTag("messaging.system", "websocket");
                activity?.SetTag("messaging.destination.name", FeederConfiguration.Path);
                activity?.SetTag("messaging.operation", "receive");

                var outcome = await _inboxCoordinator.ReceiveAsync(
                    bytes,
                    "application/octet-stream",
                    headers: null,
                    partitionKey: null,
                    invokeHandlerAsync: token => ReceiveAsync(webSocketFeederMessage, activityContext, baggage, cancellationToken: token),
                    acknowledgeAsync: _ => ValueTask.CompletedTask,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                switch (outcome)
                {
                    case InboxReceiveOutcome.Disabled:
                    case InboxReceiveOutcome.Processed:
                        WebSocketFeederExtensions.MessagesReceived.Add(1);
                        break;

                    case InboxReceiveOutcome.Duplicate:
                    case InboxReceiveOutcome.AlreadyDeadLettered:
                    case InboxReceiveOutcome.InProgress:
                        // Nothing to acknowledge for this transport - a duplicate/in-progress/already
                        // dead-lettered delivery is not a fault, just nothing new to do.
                        break;

                    case InboxReceiveOutcome.Failed:
                    case InboxReceiveOutcome.DeadLettered:
                        // The Inbox retry worker owns recovery from here.
                        WebSocketFeederExtensions.MessagesReceiveFailed.Add(1);
                        ReportHealth(HealthStatus.Degraded);
                        Log.InboxProcessingFailed(Logger, FeederConfiguration.Path, outcome.ToString());
                        break;
                }
            }
            catch (Exception exception)
            {
                ReportHealth(HealthStatus.Unhealthy, exception);

                Log.EnqueueError(Logger, exception);

                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                WebSocketFeederExtensions.MessagesReceiveFailed.Add(1);
            }
            finally
            {
                WebSocketFeederExtensions.ReceiveDuration.Record(Stopwatch.GetElapsedTime(receiveTimestamp).TotalMilliseconds);
                activity?.Dispose();
            }
        }
    }
}