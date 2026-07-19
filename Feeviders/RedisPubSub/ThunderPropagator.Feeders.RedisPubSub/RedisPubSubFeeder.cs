using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using StackExchange.Redis;
using System.Reflection;

namespace ThunderPropagator.Feeders.RedisPubSub
{
    internal
#if !DEBUG
        sealed
#endif
        class RedisPubSubFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration> : DelegativeFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
        where TChannel : class, IChannel
        where TRedisPubSubFeederMessage : RedisPubSubFeederMessage
        where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration
    {
        private readonly TRedisPubSubFeederConfiguration _redisPubSubFeederConfiguration;
        private IConnectionMultiplexer? _connectionMultiplexer;
        private readonly RedisChannel _redisChannel;
        private ChannelMessageQueue? _messageQueue;

        public RedisPubSubFeeder(TChannel channel,
            TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration,
            IFeederHandler<TChannel, TRedisPubSubFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, redisPubSubFeederConfiguration, feederHandler, serviceProvider)
        {
            _redisPubSubFeederConfiguration = redisPubSubFeederConfiguration;
            _redisChannel = new RedisChannel(_redisPubSubFeederConfiguration.Channel, _redisPubSubFeederConfiguration.PatternMode);

            HealthName = $"feeder_{nameof(RedisPubSub)}_{_redisPubSubFeederConfiguration.Channel}";
            HealthTags = [.. HealthTags, nameof(RedisPubSub), _redisPubSubFeederConfiguration.Channel];
        }

        protected override async Task StartAsync(CancellationToken cancellationToken = default)
        {
            var connectionMultiplexer = await ConnectionMultiplexer
                .ConnectAsync(_redisPubSubFeederConfiguration.ConnectionString)
                .ConfigureAwait(false);

            try
            {
                var messageQueue = await connectionMultiplexer
                    .GetSubscriber()
                    .SubscribeAsync(_redisChannel)
                    .ConfigureAwait(false);

                messageQueue.OnMessage(message => RedisPubSubMessageHandler.ProcessAsync(
                    message,
                    ProcessMessageAsync,
                    HandleProcessingError));

                _connectionMultiplexer = connectionMultiplexer;
                _messageQueue = messageQueue;
            }
            catch
            {
                await connectionMultiplexer.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            Logger.LogInformation(
                "{Name}/{ChannelName} on Channel {Channel} has configured.",
                GetType().GetTypeInfo().Name,
                Channel.Metadata.ChannelName,
                _redisPubSubFeederConfiguration.Channel);
        }

        private async Task ProcessMessageAsync(ChannelMessage channelMessage)
        {
            var message = channelMessage.Message;
            if (message.IsNullOrEmpty)
                return;

            // Prefer binary path to avoid string allocations when the publisher sent raw bytes
            TRedisPubSubFeederMessage? redisPubSubFeederMessage = null;

            try
            {
                // Attempt to get raw bytes - if the message was published as bytes this avoids encoding allocations
                var bytes = (byte[]?)message;
                if (bytes is not null && bytes.Length > 0)
                {
                    redisPubSubFeederMessage = Deserialize(bytes);
                }
            }
            catch (InvalidCastException exception)
            {
                Logger.LogDebug(exception,
                    "Failed to cast message to bytes on Channel {Channel}, falling back to string parsing.",
                    _redisPubSubFeederConfiguration.Channel);

                // Fall back to string path
                var strMessage = message.ToString();
                if (string.IsNullOrWhiteSpace(strMessage))
                    return;

                redisPubSubFeederMessage = Deserialize(strMessage);
            }

            if (redisPubSubFeederMessage is null)
                throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

            var activityContext = redisPubSubFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
            var baggage = redisPubSubFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
            await ReceiveAsync(redisPubSubFeederMessage, activityContext, baggage).ConfigureAwait(false);

            ReportHealth(HealthStatus.Healthy);
        }

        private void HandleProcessingError(Exception exception)
        {
            ReportHealth(HealthStatus.Unhealthy, exception);
            Logger.LogError(exception, "error has occured while consuming messages on Channel {Channel}.", _redisPubSubFeederConfiguration.Channel);
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            var messageQueue = Interlocked.Exchange(ref _messageQueue, null);
            if (messageQueue is not null)
                await messageQueue.UnsubscribeAsync().ConfigureAwait(false);

            if (_connectionMultiplexer is not null)
                await _connectionMultiplexer.CloseAsync().ConfigureAwait(false);
        }

        protected override ValueTask DisposeManagedResourcesAsync()
        {
            var connectionMultiplexer = Interlocked.Exchange(ref _connectionMultiplexer, null);
            return connectionMultiplexer?.DisposeAsync() ?? ValueTask.CompletedTask;
        }
    }
}
