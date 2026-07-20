using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Net;
using OpenTelemetry;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Feeviders.NATS.SharedKernel;

namespace ThunderPropagator.Feeders.NATS
{
    internal
#if !DEBUG
        sealed
#endif
        partial class NatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration> : IterativeFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TNatsFeederMessage : NatsFeederMessage
        where TNatsFeederConfiguration : NatsFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4300, Level = LogLevel.Warning, Message = "{FeederName}/{ChannelName} is disabled (IsEnabled=false), skipping broker connection.")]
            public static partial void FeederDisabled(ILogger logger, string feederName, string channelName);

            [LoggerMessage(EventId = 4301, Level = LogLevel.Error, Message = "Received a NATS message with no data on Subject {Subject}; message dropped.")]
            public static partial void MessageWithNoData(ILogger logger, Exception? exception, string subject);

            [LoggerMessage(EventId = 4302, Level = LogLevel.Error, Message = "Received a NATS JetStream message with no data on Stream {StreamName}; acknowledging to prevent redelivery of a poison message.")]
            public static partial void JetStreamMessageWithNoData(ILogger logger, Exception? exception, string? streamName);

            [LoggerMessage(EventId = 4303, Level = LogLevel.Error, Message = "Failed to initialize JetStream consumer.")]
            public static partial void JetStreamConsumerInitializationFailed(ILogger logger, Exception exception);
        }

        private readonly INatsClient _client;
        private INatsJSConsumer? _natsJsConsumer;
        private Exception? _jetStreamInitializationException;
        private bool _isJetStreamReady;

        public NatsFeeder(TChannel channel,
            TNatsFeederConfiguration feederConfiguration,
            IFeederHandler<TChannel, TNatsFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, feederConfiguration, feederHandler, serviceProvider)
        {
            _client = NatsClientFactory.CreateClient(feederConfiguration, serviceProvider.GetRequiredService<ILoggerFactory>());

            if (feederConfiguration.MessagingType == MessagingType.JetStream)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(FeederConfiguration.StreamName);
                ArgumentNullException.ThrowIfNull(FeederConfiguration.ConsumerConfig);
            }
        }

        protected override async Task StartingAsync(CancellationToken cancellationToken = default)
        {
            if (!FeederConfiguration.IsEnabled)
            {
                Log.FeederDisabled(Logger, GetType().Name, Channel.Metadata.ChannelName);
                return;
            }
            else if (FeederConfiguration.MessagingType == MessagingType.JetStream)
            {
                try
                {
                    await InitializeJetStreamConsumerAsync(cancellationToken).ConfigureAwait(false);
                    _isJetStreamReady = true;
                }
                catch (Exception exception)
                {
                    _jetStreamInitializationException = exception;
                    throw;
                }
            }

            await base.StartingAsync(cancellationToken).ConfigureAwait(false);
        }

        protected override async IAsyncEnumerable<FeederReceivedMessage<TNatsFeederMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = new())
        {
            switch (FeederConfiguration.MessagingType)
            {
                case MessagingType.Basic:
                    await foreach (var message in _client.SubscribeAsync<TNatsFeederMessage>(FeederConfiguration.Subject,
                                       queueGroup: FeederConfiguration.QueueGroup,
                                       cancellationToken: cancellationToken))
                    {
                        if (message.Data is not null)
                        {
                            yield return MessageConsumed(message.Data, message.Headers);
                        }
                        else
                        {
                            Log.MessageWithNoData(Logger, message.Error, FeederConfiguration.Subject);
                            ReportHealth(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Degraded, message.Error);
                        }
                    }

                    break;
                case MessagingType.JetStream:
                    if (!_isJetStreamReady)
                    {
                        var exception = _jetStreamInitializationException ??
                                        new InvalidOperationException("The JetStream consumer has not been initialized.");
                        ReportHealth(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Unhealthy, exception);
                        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
                            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                        break;
                    }

                    ArgumentNullException.ThrowIfNull(_natsJsConsumer);

                    await foreach (var message in _natsJsConsumer.ConsumeAsync<TNatsFeederMessage>(cancellationToken: cancellationToken))
                    {
                        if (message.Data is null)
                        {
                            Log.JetStreamMessageWithNoData(Logger, message.Error, FeederConfiguration.StreamName);
                            ReportHealth(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Degraded, message.Error);

                            await NatsJetStreamMessageSettlement
                                .AckOrNakAsync(message, Logger, cancellationToken)
                                .ConfigureAwait(false);
                            continue;
                        }

                        await foreach (var receivedMessage in NatsJetStreamMessageSettlement.YieldAndSettleAsync(
                                           message,
                                           MessageConsumed(message.Data, message.Headers),
                                           Logger,
                                           cancellationToken))
                        {
                            yield return receivedMessage;
                        }
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            yield break;

            FeederReceivedMessage<TNatsFeederMessage> MessageConsumed(TNatsFeederMessage message, NatsHeaders? headers)
            {
                ActivityContext? activityContext = null;
                if (headers?.TryGetValue(nameof(ActivityContext), out var activityContextStr) == true)
                    activityContext = activityContextStr.ToString().FromNJsonBase64<ActivityContext>();

                Baggage? baggage = null;
                if (headers?.TryGetValue(nameof(Baggage), out var baggageStr) == true)
                    baggage = baggageStr.ToString().FromNJsonBase64<Baggage>();

                return new FeederReceivedMessage<TNatsFeederMessage>(message, activityContext, baggage);
            }
        }

        private async Task InitializeJetStreamConsumerAsync(CancellationToken cancellationToken)
        {
            try
            {
                _natsJsConsumer = await _client.CreateJetStreamContext()
                    .CreateOrUpdateConsumerAsync(FeederConfiguration.StreamName!,
                        FeederConfiguration.ConsumerConfig!,
                        cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.JetStreamConsumerInitializationFailed(Logger, ex);
                throw;
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _client.DisposeAsync();
        }
    }
}
