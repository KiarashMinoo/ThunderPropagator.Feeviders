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
        class NatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration> : IterativeFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TNatsFeederMessage : NatsFeederMessage
        where TNatsFeederConfiguration : NatsFeederConfiguration
    {
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
            if (FeederConfiguration.MessagingType == MessagingType.JetStream)
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
                            yield return MessageConsumed(message.Data, message.Headers);
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
                            await message.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
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
                Logger.LogError(ex, "Failed to initialize JetStream consumer.");
                throw;
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _client.DisposeAsync();
        }
    }
}
