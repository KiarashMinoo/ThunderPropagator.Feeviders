using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Net;
using OpenTelemetry;
using RapidStreamer.Application;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Feeviders.NATS.SharedKernel;

namespace RapidStreamer.Feeders.NATS
{
    internal
#if !DEBUG
        sealed
#endif
        class NatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration> : IterativeFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TNatsFeederMessage : NatsFeederMessage
        where TNatsFeederConfiguration : NatsFeederConfiguration
    {
        private readonly INatsClient _client;
        private readonly INatsJSConsumer? _natsJsConsumer;

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

                _natsJsConsumer = _client.CreateJetStreamContext()
                    .CreateOrUpdateConsumerAsync(FeederConfiguration.StreamName,
                        FeederConfiguration.ConsumerConfig,
                        serviceProvider.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping)
                    .GetAwaiter()
                    .GetResult();
            }
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
                            yield return MessageConsumed(message.Data, message.Headers);
                    }

                    break;
                case MessagingType.JetStream:

                    ArgumentNullException.ThrowIfNull(_natsJsConsumer);

                    await foreach (var message in _natsJsConsumer.ConsumeAsync<TNatsFeederMessage>(cancellationToken: cancellationToken))
                    {
                        if (message.Data is not null)
                            yield return MessageConsumed(message.Data, message.Headers);

                        await message.AckAsync(cancellationToken: cancellationToken);
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            yield break;

            FeederReceivedMessage<TNatsFeederMessage> MessageConsumed(TNatsFeederMessage message, NatsHeaders? headers)
            {
#if DEBUG
                ActivityContext? activityContext = null;
                if (headers?.TryGetValue(nameof(ActivityContext), out var activityContextStr) == true)
                    activityContext = activityContextStr.ToString().FromNJsonBase64<ActivityContext>();

                Baggage? baggage = null;
                if (headers?.TryGetValue(nameof(Baggage), out var baggageStr) == true)
                    baggage = baggageStr.ToString().FromNJsonBase64<Baggage>();

                return new FeederReceivedMessage<TNatsFeederMessage>(message, activityContext, baggage);
#else
                return message;
#endif
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _client.DisposeAsync();
        }
    }
}