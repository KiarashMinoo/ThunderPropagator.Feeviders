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
        private readonly IConnectionMultiplexer _connectionMultiplexer;
        private readonly RedisChannel _redisChannel;
        private readonly ChannelMessageQueue _messageQueue;

        public RedisPubSubFeeder(TChannel channel,
            TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration,
            IFeederHandler<TChannel, TRedisPubSubFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, redisPubSubFeederConfiguration, feederHandler, serviceProvider)
        {
            _redisPubSubFeederConfiguration = redisPubSubFeederConfiguration;
            _connectionMultiplexer = ConnectionMultiplexer.Connect(_redisPubSubFeederConfiguration.ConnectionString);
            _redisChannel = new RedisChannel(_redisPubSubFeederConfiguration.Channel, _redisPubSubFeederConfiguration.PatternMode);

            _messageQueue = _connectionMultiplexer.GetSubscriber().Subscribe(_redisChannel);
            _messageQueue.OnMessage(message => RedisPubSubMessageHandler.ProcessAsync(
                message,
                ProcessMessageAsync,
                HandleProcessingError));

            Logger.LogInformation("{Name}/{ChannelName} on Channel {Channel} has configured.", GetType().GetTypeInfo().Name, channel.Metadata.ChannelName,
                _redisPubSubFeederConfiguration.Channel);

            HealthName = $"feeder_{nameof(RedisPubSub)}_{_redisPubSubFeederConfiguration.Channel}";
            HealthTags = [.. HealthTags, nameof(RedisPubSub), _redisPubSubFeederConfiguration.Channel];
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
            catch
            {
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
            await _messageQueue.UnsubscribeAsync();
            await _connectionMultiplexer.CloseAsync();
        }

        protected override ValueTask DisposeManagedResourcesAsync()
        {
            return _connectionMultiplexer.DisposeAsync();
        }
    }
}
