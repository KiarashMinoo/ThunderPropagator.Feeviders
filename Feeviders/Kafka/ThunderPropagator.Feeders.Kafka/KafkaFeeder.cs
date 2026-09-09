using System.Runtime.CompilerServices;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeders.Kafka
{
    internal
#if !DEBUG
        sealed
#endif
        partial class KafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration> : IterativeFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
        where TChannel : class, IChannel
        where TKafkaFeederMessage : KafkaFeederMessage
        where TKafkaFeederConfiguration : KafkaFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4002, Level = LogLevel.Warning, Message = "{FeederName}/{ChannelName} is disabled (IsEnabled=false), skipping broker connection.")]
            public static partial void FeederDisabled(ILogger logger, string feederName, string channelName);

            [LoggerMessage(EventId = 4003, Level = LogLevel.Error, Message = "Error: {Reason}")]
            public static partial void ConsumerErrorHandler(ILogger logger, string reason);

            [LoggerMessage(EventId = 4004, Level = LogLevel.Information, Message = "{FeederName}/{ChannelName} on topic(s) {TopicNames} has subscribed.")]
            public static partial void Subscribed(ILogger logger, string feederName, string channelName, string[] topicNames);

            [LoggerMessage(EventId = 4005, Level = LogLevel.Information, Message = "Reached end of topic {Topic}, partition {Partition}, offset {Offset}.")]
            public static partial void ReachedPartitionEof(ILogger logger, string topic, Partition partition, Offset offset);

            [LoggerMessage(EventId = 4006, Level = LogLevel.Error, Message = "error has occured while consuming messages on topics {Topics}, Error = {Error}.")]
            public static partial void ConsumeKafkaException(ILogger logger, Exception exception, string[] topics, Error error);

            [LoggerMessage(EventId = 4007, Level = LogLevel.Error, Message = "error has occured while consuming messages on topics {Topics}.")]
            public static partial void ConsumeException(ILogger logger, Exception exception, string[] topics);

            [LoggerMessage(EventId = 4008, Level = LogLevel.Warning, Message = "Exception while closing Kafka consumer.")]
            public static partial void CloseConsumerException(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4009, Level = LogLevel.Warning, Message = "Exception while disposing Kafka consumer.")]
            public static partial void DisposeConsumerException(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4010, Level = LogLevel.Warning, Message = "Exception while disposing schema registry.")]
            public static partial void DisposeSchemaRegistryException(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4011, Level = LogLevel.Error, Message = "Inbox processing on topic(s) {TopicNames} ended as {Outcome}.")]
            public static partial void InboxProcessingFailed(ILogger logger, string[] topicNames, string outcome);
        }

        private readonly IConsumer<string, TKafkaFeederMessage>? _consumer;
        private readonly TKafkaFeederConfiguration _kafkaFeederConfiguration;
        private readonly InboxReceiveCoordinator _inboxCoordinator;
        private CachedSchemaRegistryClient? _schemaRegistry;

        private ISchemaRegistryClient SchemaRegistryClient
            => _schemaRegistry = _schemaRegistry switch
            {
                null when string.IsNullOrWhiteSpace(_kafkaFeederConfiguration.SchemaRegistryUrl) => throw new InvalidOperationException("The `SchemaRegistryUrl` is required"),
                null => new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = _kafkaFeederConfiguration.SchemaRegistryUrl }),
                _ => _schemaRegistry
            };

        public KafkaFeeder(TChannel channel,
            TKafkaFeederConfiguration kafkaFeederConfiguration,
            IFeederHandler<TChannel, TKafkaFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, kafkaFeederConfiguration, feederHandler, serviceProvider)
        {
            _kafkaFeederConfiguration = kafkaFeederConfiguration;
            _inboxCoordinator = new InboxReceiveCoordinator(
                kafkaFeederConfiguration.Inbox, ChannelKey, Id, serviceProvider.GetService<IInboxStoreFactory>());

            if (!_kafkaFeederConfiguration.IsEnabled)
            {
                Log.FeederDisabled(Logger, GetType().Name, channel.Metadata.ChannelName);
                return;
            }

            HealthName = $"feeder_{nameof(Kafka)}_{_kafkaFeederConfiguration.GroupId}_{string.Join("_", _kafkaFeederConfiguration.TopicNames.Select(topicName => topicName))}";
            HealthTags = [.. HealthTags, nameof(Kafka), .. _kafkaFeederConfiguration.TopicNames];

            var consumerConfig = _kafkaFeederConfiguration.ToConsumerConfig();
            if (_inboxCoordinator.InboxEnabled)
                // Inbox claims/acks ownership of the offset explicitly (see ReceiveAsync/CommitAsync
                // below) - the background auto-committer must not race ahead of the durable claim.
                consumerConfig.EnableAutoCommit = false;

            var formatDeserializerInvoker = serviceProvider.GetRequiredService<FormatDeserializerInvoker>();

            _consumer = KafkaFeederInitializer.Initialize(
                () => new ConsumerBuilder<string, TKafkaFeederMessage>(consumerConfig)
                    .SetKeyDeserializer(Deserializers.Utf8)
                    .SetValueDeserializer(new KafkaDeserializer<TKafkaFeederMessage>(formatDeserializerInvoker, this, _kafkaFeederConfiguration.SerializerType).AsSyncOverAsync())
                    .SetErrorHandler((_, e) =>
                    {
                        ReportHealth(HealthStatus.Unhealthy, new KafkaException(e));
                        Log.ConsumerErrorHandler(Logger, e.Reason);
                    })
                    .Build(),
                consumer => consumer.Subscribe(_kafkaFeederConfiguration.TopicNames),
                () => _schemaRegistry?.Dispose());

            Log.Subscribed(Logger, GetType().Name, channel.Metadata.ChannelName, _kafkaFeederConfiguration.TopicNames);
        }

        protected override async IAsyncEnumerable<FeederReceivedMessage<TKafkaFeederMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (_consumer is null)
            {
                await Task.Yield();
                yield break;
            }

            var consumeResult = await BlockingOperationRunner.RunAsync(
                () => _consumer.Consume(cancellationToken),
                cancellationToken).ConfigureAwait(false);

            if (consumeResult is not null)
            {
                if (consumeResult.IsPartitionEOF)
                {
                    Log.ReachedPartitionEof(Logger, consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

                    await Task.Yield();
                }

                else
                {
                    var message = consumeResult.Message.Value;

                    if (message is not null)
                    {
                        ActivityContext? activityContext = null;
                        if (consumeResult.Message.Headers.TryGetLastBytes(nameof(Activity), out var activityContextBytes) && activityContextBytes is not null)
                            activityContext = activityContextBytes.FromNJsonBytes<ActivityContext>();

                        Baggage? baggage = null;
                        if (consumeResult.Message.Headers.TryGetLastBytes(nameof(Baggage), out var baggageBytes) && baggageBytes is not null)
                            baggage = baggageBytes.FromNJsonBytes<Baggage>();

                        using var activity = activityContext.HasValue
                            ? KafkaFeederExtensions.ActivitySource.StartActivity("kafka receive", ActivityKind.Consumer, activityContext.Value)
                            : KafkaFeederExtensions.ActivitySource.StartActivity("kafka receive", ActivityKind.Consumer);
                        activity?.SetTag("messaging.system", "kafka");
                        activity?.SetTag("messaging.destination.name", consumeResult.Topic);
                        activity?.SetTag("messaging.operation", "receive");

                        var additionalData = new Dictionary<string, object?>
                        {
                            { nameof(consumeResult.Topic), consumeResult.Topic },
                            { nameof(consumeResult.Offset), consumeResult.Offset },
                        };

                        if (_inboxCoordinator.InboxEnabled)
                        {
                            await ReceiveThroughInboxAsync(consumeResult, message, activityContext, baggage, additionalData, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            var receiveTimestamp = Stopwatch.GetTimestamp();
                            FeederReceivedMessage<TKafkaFeederMessage> receivedMessage;
                            try
                            {
                                receivedMessage = new FeederReceivedMessage<TKafkaFeederMessage>(message, activityContext, baggage, additionalData);

                                KafkaFeederExtensions.MessagesReceived.Add(1);
                            }
                            catch (Exception ex)
                            {
                                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                                KafkaFeederExtensions.MessagesReceiveFailed.Add(1);
                                throw;
                            }
                            finally
                            {
                                KafkaFeederExtensions.ReceiveDuration.Record(Stopwatch.GetElapsedTime(receiveTimestamp).TotalMilliseconds);
                            }

                            yield return receivedMessage;
                        }
                    }
                    else
                        await Task.Yield();
                }
            }
            else
                await Task.Yield();
        }

        /// <summary>
        /// Wraps one claimed-or-not record through <see cref="InboxReceiveCoordinator.ReceiveAsync"/>
        /// instead of yielding it to the base class's dispatch loop, which knows nothing about
        /// claims/commits. Mirrors <c>RabbitMQFeeder.HandleReceivedAsync</c>'s outcome handling, adapted
        /// to Kafka's commit-based (rather than per-message ack/nack) acknowledgement model.
        /// </summary>
        private async Task ReceiveThroughInboxAsync(
            ConsumeResult<string, TKafkaFeederMessage> consumeResult,
            TKafkaFeederMessage message,
            ActivityContext? activityContext,
            Baggage? baggage,
            IReadOnlyDictionary<string, object?> additionalData,
            CancellationToken cancellationToken)
        {
            var payload = message.RawPayload ?? [];
            var headers = ExtractHeaders(consumeResult.Message.Headers);
            // Kafka has no application-level message-ID header of its own - topic/partition/offset
            // is the natural stable identity, so it is synthesized as a header the BrokerHeader
            // MessageIdResolver strategy can read (see KafkaFeederConfiguration.Inbox.MessageIdResolution).
            headers["kafka-offset"] = consumeResult.Offset.Value.ToString();
            var partitionKey = $"{consumeResult.Topic}#{consumeResult.Partition.Value}";

            var outcome = await _inboxCoordinator.ReceiveAsync(
                payload,
                "application/octet-stream",
                headers,
                partitionKey,
                invokeHandlerAsync: ct => ReceiveAsync(message, activityContext, baggage, additionalData, ct),
                acknowledgeAsync: ct => CommitOffsetAsync(consumeResult, ct),
                cancellationToken).ConfigureAwait(false);

            switch (outcome)
            {
                case InboxReceiveOutcome.Processed:
                    KafkaFeederExtensions.MessagesReceived.Add(1);
                    break;

                case InboxReceiveOutcome.Duplicate:
                case InboxReceiveOutcome.AlreadyDeadLettered:
                    // Already committed by the coordinator - a duplicate/dead-lettered delivery is
                    // not a fault, just nothing new to do.
                    break;

                case InboxReceiveOutcome.InProgress:
                    // Not committed by the coordinator - another owner holds the claim. Unlike a
                    // broker-level nack/requeue, Kafka does not redeliver mid-session on an
                    // uncommitted offset; the Inbox's own lease-expiry/retry-worker path owns
                    // recovery from here.
                    break;

                case InboxReceiveOutcome.Failed:
                case InboxReceiveOutcome.DeadLettered:
                    // Already committed by the coordinator immediately after the durable claim -
                    // the Inbox retry worker owns recovery from here, not broker redelivery.
                    KafkaFeederExtensions.MessagesReceiveFailed.Add(1);
                    Log.InboxProcessingFailed(Logger, _kafkaFeederConfiguration.TopicNames, outcome.ToString());
                    break;
            }
        }

        /// <summary>
        /// Commits <paramref name="consumeResult"/>'s offset synchronously via librdkafka - run on a
        /// dedicated long-running thread (<see cref="BlockingOperationRunner"/>) for the same reason
        /// <see cref="_consumer"/>.Consume itself is: it is a blocking client call.
        /// </summary>
        private async ValueTask CommitOffsetAsync(ConsumeResult<string, TKafkaFeederMessage> consumeResult, CancellationToken cancellationToken) =>
            await BlockingOperationRunner.RunAsync(() =>
            {
                _consumer!.Commit(consumeResult);
                return true;
            }, cancellationToken).ConfigureAwait(false);

        private static Dictionary<string, string> ExtractHeaders(Headers headers)
        {
            var result = new Dictionary<string, string>(headers.Count);
            foreach (var header in headers)
                result[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());

            return result;
        }

        protected override async Task<bool> HandleExceptionAsync(Exception exception, CancellationToken cancellationToken = default)
        {
            var awaitness = 10;
            switch (exception)
            {
                case ConsumeException consumeException when consumeException.Error.Code == ErrorCode.UnknownTopicOrPart:
                    ReportHealth(HealthStatus.Unhealthy, consumeException);
                    awaitness = 60;
                    break;
                case KafkaException kafkaException:
                {
                    ReportHealth(kafkaException.Error.IsFatal ? HealthStatus.Unhealthy : HealthStatus.Degraded, kafkaException);

                    Log.ConsumeKafkaException(Logger, kafkaException, _kafkaFeederConfiguration.TopicNames, kafkaException.Error);
                    break;
                }
                default:
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Log.ConsumeException(Logger, exception, _kafkaFeederConfiguration.TopicNames);
                    break;
            }

            await Task.Delay(TimeSpan.FromSeconds(awaitness), cancellationToken).ConfigureAwait(false);
            return true;
        }

        protected override Task StoppingAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                _consumer?.Close();
            }
            catch (Exception ex)
            {
                Log.CloseConsumerException(Logger, ex);
            }

            return base.StoppingAsync(cancellationToken);
        }

        protected override void DisposeManagedResources()
        {
            try
            {
                _consumer?.Dispose();
            }
            catch (Exception ex)
            {
                Log.DisposeConsumerException(Logger, ex);
            }

            try
            {
                _schemaRegistry?.Dispose();
            }
            catch (Exception ex)
            {
                Log.DisposeSchemaRegistryException(Logger, ex);
            }
        }
    }
}
