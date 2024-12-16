using System.Diagnostics;
using System.Runtime.CompilerServices;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using OpenTelemetry;
using RapidStreamer.Application;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.Pulsar.SharedKernel;

namespace RapidStreamer.Feeders.Pulsar
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
        private readonly IPulsarClient _client;
        private readonly IConsumer<TPulsarFeederMessage> _consumer;

        public PulsarFeeder(TChannel channel,
            TPulsarFeederConfiguration feederConfiguration,
            IFeederHandler<TChannel, TPulsarFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, feederConfiguration, feederHandler, serviceProvider)
        {
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
            await foreach (var message in _consumer.Messages(cancellationToken: cancellationToken))
            {
                var value = message.Value();
#if DEBUG
                var activityContext = value[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                var baggage = value[nameof(Baggage)] is Baggage b ? b : default;
                yield return new FeederReceivedMessage<TPulsarFeederMessage>(value, activityContext, baggage);
#else
                yield return value;
#endif
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _consumer.DisposeAsync();
            await _client.DisposeAsync();
        }
    }
}