using OpenTelemetry;
using System.Diagnostics;
using ThunderPropagator.Feeviders.RedisPubSub.SharedKernel;
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
        partial class RedisPubSubFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration> : DelegativeFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
        where TChannel : class, IChannel
        where TRedisPubSubFeederMessage : RedisPubSubFeederMessage
        where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4600, Level = LogLevel.Information, Message = "{Name}/{ChannelName} on Channel {Channel} has configured.")]
            public static partial void FeederConfigured(ILogger logger, string name, string channelName, string channel);

            [LoggerMessage(EventId = 4601, Level = LogLevel.Debug, Message = "Failed to cast message to bytes on Channel {Channel}, falling back to string parsing.")]
            public static partial void MessageCastFailed(ILogger logger, InvalidCastException exception, string channel);

            [LoggerMessage(EventId = 4602, Level = LogLevel.Error, Message = "error has occured while consuming messages on Channel {Channel}.")]
            public static partial void ConsumeError(ILogger logger, Exception exception, string channel);
        }

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

            Log.FeederConfigured(
                Logger,
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
                Log.MessageCastFailed(
                    Logger,
                    exception,
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

            using var activity = activityContext != default
                ? RedisPubSubTelemetry.ActivitySource.StartActivity("redispubsub receive", ActivityKind.Consumer, activityContext)
                : RedisPubSubTelemetry.ActivitySource.StartActivity("redispubsub receive", ActivityKind.Consumer);
            activity?.SetTag("messaging.system", "redispubsub");
            activity?.SetTag("messaging.destination.name", (string?)channelMessage.Channel);
            activity?.SetTag("messaging.operation", "receive");

            var stopwatch = Stopwatch.StartNew();
            try
            {
                await ReceiveAsync(redisPubSubFeederMessage, activityContext, baggage).ConfigureAwait(false);

                ReportHealth(HealthStatus.Healthy);
                RedisPubSubTelemetry.MessagesReceived.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                RedisPubSubTelemetry.MessagesReceiveFailed.Add(1);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                RedisPubSubTelemetry.ReceiveDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }
        }

        private void HandleProcessingError(Exception exception)
        {
            ReportHealth(HealthStatus.Unhealthy, exception);
            Log.ConsumeError(Logger, exception, _redisPubSubFeederConfiguration.Channel);
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
