using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Features;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.Feeders.WebApi
{
    internal
#if !DEBUG
        sealed
#endif
        partial class WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration> : DelegativeFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TWebApiFeederMessage : WebApiFeederMessage
        where TWebApiFeederConfiguration : WebApiFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4800, Level = LogLevel.Information, Message = "{Name}/{ChannelName} on Endpoint {Endpoint} has configured.")]
            public static partial void FeederConfigured(ILogger logger, string name, string channelName, string endpoint);

            [LoggerMessage(EventId = 4801, Level = LogLevel.Error, Message = "Error while processing a WebApi message on Endpoint {Endpoint}.")]
            public static partial void ProcessError(ILogger logger, Exception exception, string endpoint);

            [LoggerMessage(EventId = 4802, Level = LogLevel.Error, Message = "Inbox processing on Endpoint {Endpoint} ended as {Outcome}.")]
            public static partial void InboxProcessingFailed(ILogger logger, string endpoint, string outcome);
        }

        private readonly InboxReceiveCoordinator _inboxCoordinator;

        public WebApiFeeder(TChannel channel,
            TWebApiFeederConfiguration webApiFeederConfiguration,
            IFeederHandler<TChannel, TWebApiFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, webApiFeederConfiguration, feederHandler, serviceProvider)
        {
            Log.FeederConfigured(Logger, GetType().GetTypeInfo().Name, channel.Metadata.ChannelName, webApiFeederConfiguration.Path);

            HealthName = $"feeder_{nameof(WebApi)}_{webApiFeederConfiguration.Path.Replace("/", "_")}";
            HealthTags = [.. HealthTags, nameof(WebApi), webApiFeederConfiguration.Path.Replace("/", "_")];
            _inboxCoordinator = new InboxReceiveCoordinator(
                webApiFeederConfiguration.Inbox, ChannelKey, Id, serviceProvider.GetService<IInboxStoreFactory>());
        }

        internal async ValueTask EnqueueAsync(string rawMessage, string? traceparent, string? tracestate, CancellationToken cancellationToken = default)
        {
            using var activity = traceparent is not null && ActivityContext.TryParse(traceparent, tracestate, out var parentContext)
                ? WebApiFeederExtensions.ActivitySource.StartActivity("webapi receive", ActivityKind.Consumer, parentContext)
                : WebApiFeederExtensions.ActivitySource.StartActivity("webapi receive", ActivityKind.Consumer);
            activity?.SetTag("messaging.system", "webapi");
            activity?.SetTag("messaging.destination.name", FeederConfiguration.Path);
            activity?.SetTag("messaging.operation", "receive");

            var receiveTimestamp = Stopwatch.GetTimestamp();
            try
            {
                var payload = Encoding.UTF8.GetBytes(rawMessage);
                var outcome = await _inboxCoordinator.ReceiveAsync(
                    payload,
                    "text/plain; charset=utf-8",
                    headers: null,
                    partitionKey: null,
                    invokeHandlerAsync: token => ReceiveAsync(rawMessage, cancellationToken: token),
                    acknowledgeAsync: _ => ValueTask.CompletedTask,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                switch (outcome)
                {
                    case InboxReceiveOutcome.Disabled:
                    case InboxReceiveOutcome.Processed:
                        ReportHealth(HealthStatus.Healthy);
                        WebApiFeederExtensions.MessagesReceived.Add(1);
                        break;

                    case InboxReceiveOutcome.Duplicate:
                    case InboxReceiveOutcome.AlreadyDeadLettered:
                    case InboxReceiveOutcome.InProgress:
                        // Nothing to acknowledge for this transport - a duplicate/in-progress/already
                        // dead-lettered delivery is not a fault, just nothing new to do.
                        ReportHealth(HealthStatus.Healthy);
                        break;

                    case InboxReceiveOutcome.Failed:
                    case InboxReceiveOutcome.DeadLettered:
                        // The Inbox retry worker owns recovery from here - not this request.
                        WebApiFeederExtensions.MessagesReceiveFailed.Add(1);
                        ReportHealth(HealthStatus.Degraded);
                        Log.InboxProcessingFailed(Logger, FeederConfiguration.Path, outcome.ToString());
                        break;
                }
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                WebApiFeederExtensions.MessagesReceiveFailed.Add(1);
                ReportHealth(HealthStatus.Unhealthy, exception);
                Log.ProcessError(Logger, exception, FeederConfiguration.Path);
                throw;
            }
            finally
            {
                WebApiFeederExtensions.ReceiveDuration.Record(Stopwatch.GetElapsedTime(receiveTimestamp).TotalMilliseconds);
            }
        }
    }
}