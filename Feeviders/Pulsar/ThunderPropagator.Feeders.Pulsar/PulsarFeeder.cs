using System.Diagnostics;
using System.Runtime.CompilerServices;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.Pulsar.SharedKernel;

namespace ThunderPropagator.Feeders.Pulsar
{
    internal
#if !DEBUG
        sealed
#endif
        class PulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration> : IterativeFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TPulsarFeederMessage : PulsarFeederMessage
        where TPulsarFeederConfiguration : PulsarFeederConfiguration
    {
        private readonly IPulsarClient? _client;
        private readonly IConsumer<TPulsarFeederMessage>? _consumer;

        public PulsarFeeder(TChannel channel,
            TPulsarFeederConfiguration feederConfiguration,
            IFeederHandler<TChannel, TPulsarFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, feederConfiguration, feederHandler, serviceProvider)
        {
            if (!feederConfiguration.IsEnabled)
            {
                Logger.LogWarning(
                    "{FeederName}/{ChannelName} is disabled (IsEnabled=false), skipping broker connection.",
                    GetType().Name,
                    channel.Metadata.ChannelName);
                return;
            }

            _client = PulsarClientFactory.CreateClient(feederConfiguration);

            var schema = new JsonSchema<TPulsarFeederMessage>(feederConfiguration.SerializerType);

            var consumerOptions = new ConsumerOptions<TPulsarFeederMessage>(feederConfiguration.SubscriptionName, feederConfiguration.Topic, schema);

            if (!string.IsNullOrWhiteSpace(feederConfiguration.ConsumerName))
                consumerOptions.ConsumerName = feederConfiguration.ConsumerName;

            if (feederConfiguration.InitialPosition != null)
                consumerOptions.InitialPosition = feederConfiguration.InitialPosition.Value;

            if (feederConfiguration.MessagePrefetchCount != null)
                consumerOptions.MessagePrefetchCount = feederConfiguration.MessagePrefetchCount.Value;

            if (feederConfiguration.PriorityLevel != null)
                consumerOptions.PriorityLevel = feederConfiguration.PriorityLevel.Value;

            if (feederConfiguration.ReadCompacted != null)
                consumerOptions.ReadCompacted = feederConfiguration.ReadCompacted.Value;

            if (feederConfiguration.SubscriptionType != null)
                consumerOptions.SubscriptionType = feederConfiguration.SubscriptionType.Value;

            _consumer = _client.CreateConsumer(consumerOptions);
        }

        protected override async IAsyncEnumerable<FeederReceivedMessage<TPulsarFeederMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = new())
        {
            if (_consumer is null)
            {
                await Task.Yield();
                yield break;
            }

            await foreach (var message in _consumer.Messages(cancellationToken: cancellationToken))
            {
                TPulsarFeederMessage value;
                try
                {
                    value = message.Value();
                }
                catch (Exception exception)
                {
                    Logger.LogError(exception,
                        "Failed to deserialize a Pulsar message (MessageId: {MessageId}) on Topic {Topic}; redelivering instead of processing.",
                        message.MessageId,
                        _consumer.Topic);
                    ReportHealth(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Degraded, exception);

                    try
                    {
                        await _consumer.RedeliverUnacknowledgedMessages([message.MessageId], cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception redeliverException)
                    {
                        Logger.LogError(redeliverException, "Failed to redeliver an unacknowledged Pulsar message.");
                    }

                    continue;
                }

                var activityContext = value[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                var baggage = value[nameof(Baggage)] is Baggage b ? b : default;

                await foreach (var receivedMessage in PulsarMessageSettlement.YieldAndSettleAsync(
                                   _consumer,
                                   message,
                                   new FeederReceivedMessage<TPulsarFeederMessage>(value, activityContext, baggage),
                                   Logger,
                                   cancellationToken))
                {
                    yield return receivedMessage;
                }
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            if (_consumer is not null)
                await _consumer.DisposeAsync();

            if (_client is not null)
                await _client.DisposeAsync();
        }
    }
}