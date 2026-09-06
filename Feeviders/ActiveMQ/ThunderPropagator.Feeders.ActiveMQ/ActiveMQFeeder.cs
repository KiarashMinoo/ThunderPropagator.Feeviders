using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using System.Text;
using Apache.NMS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeviders.ActiveMQ.SharedKernel;
using System.Reflection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Features;

namespace ThunderPropagator.Feeders.ActiveMQ
{
    internal
#if !DEBUG
        sealed
#endif
        partial class ActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration> : DelegativeFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TActiveMQFeederMessage : ActiveMQFeederMessage
        where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4500, Level = LogLevel.Information, Message = "{Name}/{ChannelName} on Queue {Queue} has configured.")]
            public static partial void FeederConfigured(ILogger logger, string name, string channelName, string queue);

            [LoggerMessage(EventId = 4501, Level = LogLevel.Error, Message = "Exception while processing an ActiveMQ message.")]
            public static partial void MessageProcessingError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4502, Level = LogLevel.Warning, Message = "Exception while closing ActiveMQ resources.")]
            public static partial void ResourceCloseError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4503, Level = LogLevel.Warning, Message = "Exception while disposing ActiveMQ consumer.")]
            public static partial void ConsumerDisposeError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4504, Level = LogLevel.Warning, Message = "Exception while disposing ActiveMQ session.")]
            public static partial void SessionDisposeError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4505, Level = LogLevel.Warning, Message = "Exception while disposing ActiveMQ connection.")]
            public static partial void ConnectionDisposeError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4506, Level = LogLevel.Error, Message = "Inbox processing on Queue {Queue} ended as {Outcome}.")]
            public static partial void InboxProcessingFailed(ILogger logger, string queue, string outcome);
        }

        private readonly IConnection _connection;
        private readonly IMessageConsumer _consumer;
        private readonly ISession _session;
        private readonly ActiveMQMessageProcessor<IMessage> _messageProcessor;
        private readonly InboxReceiveCoordinator _inboxCoordinator;

        public ActiveMQFeeder(TChannel channel,
            TActiveMQFeederConfiguration activeMQFeederConfiguration,
            IFeederHandler<TChannel, TActiveMQFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, activeMQFeederConfiguration, feederHandler, serviceProvider)
        {
            // Create a Connection
            _connection = ActiveMQFeeviderConnectionFactory.CreateConnection(activeMQFeederConfiguration);
            _connection.Start();

            // Create a Session
            _session = _connection.CreateSession();

            // Get the destination (Topic or Queue)
            IDestination destination = _session.GetQueue(activeMQFeederConfiguration.Queue);

            // Create a MessageProducer from the Session to the Topic or Queue
            _consumer = _session.CreateConsumer(destination);

            _messageProcessor = new ActiveMQMessageProcessor<IMessage>(ProcessMessageAsync, HandleProcessingError);
            _consumer.Listener += HandleMessage;
            _inboxCoordinator = new InboxReceiveCoordinator(
                activeMQFeederConfiguration.Inbox, ChannelKey, Id, serviceProvider.GetService<IInboxStoreFactory>());

            Log.FeederConfigured(Logger, GetType().GetTypeInfo().Name, channel.Metadata.ChannelName,
                activeMQFeederConfiguration.Queue);

            HealthName = $"feeder_{nameof(ActiveMQ)}_{activeMQFeederConfiguration.Queue}";
            HealthTags = [.. HealthTags, nameof(ActiveMQ), activeMQFeederConfiguration.Queue];
        }

        private void HandleMessage(IMessage message) => _messageProcessor.Enqueue(message);

        private async Task ProcessMessageAsync(IMessage message)
        {
            ActivityContext? activityContext = null;
            if (message.Properties.Contains(nameof(ActivityContext)))
                activityContext = message.Properties.GetBytes(nameof(ActivityContext)).FromNJsonBytes<ActivityContext>();

            Baggage? baggage = null;
            if (message.Properties.Contains(nameof(Baggage)))
                baggage = message.Properties.GetBytes(nameof(Baggage)).FromNJsonBytes<Baggage>();

            using var activity = activityContext.HasValue
                ? ActiveMQTelemetry.ActivitySource.StartActivity("activemq receive", ActivityKind.Consumer, activityContext.Value)
                : ActiveMQTelemetry.ActivitySource.StartActivity("activemq receive", ActivityKind.Consumer);
            activity?.SetTag("messaging.system", "activemq");
            activity?.SetTag("messaging.destination.name", message.NMSDestination?.ToString() ?? FeederConfiguration.Queue);
            activity?.SetTag("messaging.operation", "receive");

            var stopwatch = Stopwatch.StartNew();
            try
            {
                switch (message)
                {
                    case IObjectMessage { Body: TActiveMQFeederMessage activeMQFeederMessage }:
                        // An IObjectMessage arrives already deserialized - there is no cheap raw-bytes
                        // form to persist as an Inbox claim's payload, so this path bypasses the Inbox
                        // entirely and keeps the direct handler call, same as before this feature existed.
                        await ReceiveAsync(activeMQFeederMessage, activityContext, baggage).ConfigureAwait(false);
                        ActiveMQTelemetry.MessagesReceived.Add(1);
                        break;

                    case ITextMessage textMessage:
                        var text = textMessage.Text ?? string.Empty;
                        await ProcessViaInboxAsync(
                            Encoding.UTF8.GetBytes(text),
                            "text/plain",
                            token => ReceiveAsync(text, activityContext, baggage, cancellationToken: token)).ConfigureAwait(false);
                        break;

                    case IBytesMessage bytesMessage:
                        await ProcessViaInboxAsync(
                            bytesMessage.Content,
                            "application/octet-stream",
                            token => ReceiveAsync(bytesMessage.Content, activityContext, baggage, cancellationToken: token)).ConfigureAwait(false);
                        break;
                }
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                ActiveMQTelemetry.MessagesReceiveFailed.Add(1);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                ActiveMQTelemetry.ReceiveDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }
        }

        private async Task ProcessViaInboxAsync(byte[] payload, string payloadContentType, Func<CancellationToken, ValueTask> invokeHandlerAsync)
        {
            var outcome = await _inboxCoordinator.ReceiveAsync(
                payload,
                payloadContentType,
                headers: null,
                partitionKey: null,
                invokeHandlerAsync: invokeHandlerAsync,
                acknowledgeAsync: _ => ValueTask.CompletedTask).ConfigureAwait(false);

            switch (outcome)
            {
                case InboxReceiveOutcome.Disabled:
                case InboxReceiveOutcome.Processed:
                    ActiveMQTelemetry.MessagesReceived.Add(1);
                    break;

                case InboxReceiveOutcome.Duplicate:
                case InboxReceiveOutcome.AlreadyDeadLettered:
                case InboxReceiveOutcome.InProgress:
                    // No broker-level acknowledgement/redelivery control is exposed here (this consumer
                    // uses the session's implicit ack mode) - nothing more to do than skip the handler,
                    // which the coordinator already did.
                    break;

                case InboxReceiveOutcome.Failed:
                case InboxReceiveOutcome.DeadLettered:
                    ActiveMQTelemetry.MessagesReceiveFailed.Add(1);
                    Log.InboxProcessingFailed(Logger, FeederConfiguration.Queue, outcome.ToString());
                    break;
            }
        }

        private void HandleProcessingError(Exception exception)
        {
            ReportHealth(HealthStatus.Unhealthy, exception);
            Log.MessageProcessingError(Logger, exception);
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            _consumer.Listener -= HandleMessage;
            await _messageProcessor.CompleteAsync().ConfigureAwait(false);

            try
            {
                await _consumer.CloseAsync().ConfigureAwait(false);
                await _session.CloseAsync().ConfigureAwait(false);
                await _connection.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.ResourceCloseError(Logger, ex);
            }
        }

        protected override void DisposeManagedResources()
        {
            _consumer.Listener -= HandleMessage;
            _messageProcessor.Complete();

            try
            {
                _consumer?.Dispose();
            }
            catch (Exception ex)
            {
                Log.ConsumerDisposeError(Logger, ex);
            }

            try
            {
                _session?.Dispose();
            }
            catch (Exception ex)
            {
                Log.SessionDisposeError(Logger, ex);
            }

            try
            {
                _connection?.Dispose();
            }
            catch (Exception ex)
            {
                Log.ConnectionDisposeError(Logger, ex);
            }
        }
    }
}
