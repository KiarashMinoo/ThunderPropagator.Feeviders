using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RapidStreamer.Providers.DotNet.SharedKernel;
using StackExchange.Redis;

namespace RapidStreamer.Providers.DotNet.RedisPubSub
{
    internal
#if !DEBUG
        sealed
#endif
        class RedisPubSubProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration> : AbstractProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration>
        where TRedisPubSubProviderMessage : RedisPubSubProviderMessage
        where TRedisPubSubProviderConfiguration : RedisPubSubProviderConfiguration
    {
        private readonly IConnectionMultiplexer _connectionMultiplexer;
        private readonly RedisChannel _redisChannel;
        private readonly TRedisPubSubProviderConfiguration _redisPubSubProviderConfiguration;
        private readonly ISubscriber _subscriber;

        public RedisPubSubProvider(TRedisPubSubProviderConfiguration redisPubSubProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _redisPubSubProviderConfiguration = redisPubSubProviderConfiguration;
            _connectionMultiplexer = ConnectionMultiplexer.Connect(_redisPubSubProviderConfiguration.ConnectionString);
            _subscriber = _connectionMultiplexer.GetSubscriber();
            _redisChannel = new RedisChannel(_redisPubSubProviderConfiguration.Channel, _redisPubSubProviderConfiguration.PatternMode);
        }

        protected override Task InternalExecuteAsync(TRedisPubSubProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            if (Activity.Current?.Context is not null)
                feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

            feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());

            return base.InternalExecuteAsync(feederMessage, cancellationToken);
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            try
            {
                await _subscriber.PublishAsync(_redisChannel, bytes, CommandFlags.FireAndForget);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while publishing message to channel {Channel}.",
                    _redisPubSubProviderConfiguration.Channel);
                throw;
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _subscriber.UnsubscribeAsync(_redisChannel);
            await _connectionMultiplexer.CloseAsync();
            await _connectionMultiplexer.DisposeAsync();
        }
    }
}