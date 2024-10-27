#if DEBUG
using OpenTelemetry;
using System.Diagnostics;
#endif
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using StackExchange.Redis;
using System.Reflection;

namespace RapidStreamer.Feeders.RedisPubSub
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
        private readonly ISubscriber _subscriber;

        public RedisPubSubFeeder(TChannel channel,
            TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration,
            IFeederHandler<TChannel, TRedisPubSubFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, redisPubSubFeederConfiguration, feederHandler, serviceProvider)
        {
            _redisPubSubFeederConfiguration = redisPubSubFeederConfiguration;
            _connectionMultiplexer = ConnectionMultiplexer.Connect(_redisPubSubFeederConfiguration.ConnectionString);
            _subscriber = _connectionMultiplexer.GetSubscriber();
            _redisChannel = new RedisChannel(_redisPubSubFeederConfiguration.Channel, _redisPubSubFeederConfiguration.PatternMode);

            _subscriber.Subscribe(_redisChannel, Handler);

            Logger.LogInformation("{Name}/{ChannelName} on Channel {Channel} has configured.", GetType().GetTypeInfo().Name, channel.Metadata.ChannelName,
                _redisPubSubFeederConfiguration.Channel);

            HealthName = $"feeder_{nameof(RedisPubSub)}_{_redisPubSubFeederConfiguration.Channel}";
            HealthTags = [.. HealthTags, nameof(RedisPubSub), _redisPubSubFeederConfiguration.Channel];
        }

        private async void Handler(RedisChannel _, RedisValue message)
        {
            try
            {
                var strMessage = message.ToString();
                if (string.IsNullOrWhiteSpace(strMessage))
                {
                    return;
                }

                var redisPubSubFeederMessage = Deserialize(strMessage);

#if DEBUG
                var activityContext = redisPubSubFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                var baggage = redisPubSubFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                await ReceiveAsync(redisPubSubFeederMessage, activityContext, baggage);
#else
                await ReceiveAsync(redisPubSubFeederMessage);
#endif

                ReportHealth(HealthStatus.Healthy);
            }
            catch (Exception exception)
            {
                ReportHealth(HealthStatus.Unhealthy, exception);

                Logger.LogError(exception, "error has occured while consuming messages on Channel {Channel}.", _redisPubSubFeederConfiguration.Channel);
            }
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            await _subscriber.UnsubscribeAsync(_redisChannel);
            await _connectionMultiplexer.CloseAsync();
        }

        protected override ValueTask DisposeManagedResourcesAsync()
        {
            return _connectionMultiplexer.DisposeAsync();
        }
    }
}