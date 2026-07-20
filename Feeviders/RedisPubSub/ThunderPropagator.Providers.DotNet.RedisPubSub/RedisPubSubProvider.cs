using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using StackExchange.Redis;
using System.Buffers;

namespace ThunderPropagator.Providers.DotNet.RedisPubSub
{
    internal
#if !DEBUG
        sealed
#endif
        partial class RedisPubSubProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration> : AbstractProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration>
        where TRedisPubSubProviderMessage : RedisPubSubProviderMessage
        where TRedisPubSubProviderConfiguration : RedisPubSubProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4603, Level = LogLevel.Warning, Message = "Redis publish task faulted for channel {Channel}")]
            public static partial void PublishTaskFaulted(ILogger logger, AggregateException exception, string channel);

            [LoggerMessage(EventId = 4604, Level = LogLevel.Error, Message = "error has occured while publishing message to channel {Channel}.")]
            public static partial void PublishError(ILogger logger, Exception exception, string channel);
        }

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

            // Validate connection
            if (!_connectionMultiplexer.IsConnected)
            {
                throw new InvalidOperationException($"Failed to connect to Redis at {_redisPubSubProviderConfiguration.ConnectionString}");
            }
        }

        protected override Task InternalExecuteAsync(TRedisPubSubProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            if (Activity.Current?.Context is not null)
                feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

            feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());

            return base.InternalExecuteAsync(feederMessage, cancellationToken);
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            if (bytes is null || bytes.Length == 0)
                return Task.CompletedTask;

            using var activity = RedisPubSubProviderTelemetry.ActivitySource.StartActivity("redispubsub publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "redispubsub");
            activity?.SetTag("messaging.destination.name", (string?)_redisChannel);
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();

            try
            {
                // Fire-and-forget publish: do not await to reduce latency. Attach a continuation to log any unexpected faults.
                var publishTask = _subscriber.PublishAsync(_redisChannel, bytes, CommandFlags.FireAndForget);
                _ = publishTask.ContinueWith(t =>
                {
                    if (t.IsFaulted && t.Exception is not null)
                    {
                        Log.PublishTaskFaulted(Logger, t.Exception, _redisPubSubProviderConfiguration.Channel);
                        activity?.SetStatus(ActivityStatusCode.Error, t.Exception.GetBaseException().Message);
                        RedisPubSubProviderTelemetry.MessagesPublishFailed.Add(1);
                    }
                }, TaskScheduler.Default);

                RedisPubSubProviderTelemetry.MessagesPublished.Add(1);
                return Task.CompletedTask;
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                RedisPubSubProviderTelemetry.MessagesPublishFailed.Add(1);

                Log.PublishError(
                    Logger,
                    exception,
                    _redisPubSubProviderConfiguration.Channel);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                RedisPubSubProviderTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
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