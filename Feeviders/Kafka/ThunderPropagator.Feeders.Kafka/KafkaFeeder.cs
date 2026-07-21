using System.Runtime.CompilerServices;
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
        }

        private readonly IConsumer<string, TKafkaFeederMessage>? _consumer;
        private readonly TKafkaFeederConfiguration _kafkaFeederConfiguration;
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

            if (!_kafkaFeederConfiguration.IsEnabled)
            {
                Log.FeederDisabled(Logger, GetType().Name, channel.Metadata.ChannelName);
                return;
            }

            HealthName = $"feeder_{nameof(Kafka)}_{_kafkaFeederConfiguration.GroupId}_{string.Join("_", _kafkaFeederConfiguration.TopicNames.Select(topicName => topicName))}";
            HealthTags = [.. HealthTags, nameof(Kafka), .. _kafkaFeederConfiguration.TopicNames];

            var consumerConfig = _kafkaFeederConfiguration.ToConsumerConfig();
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

                        var receiveTimestamp = Stopwatch.GetTimestamp();
                        FeederReceivedMessage<TKafkaFeederMessage> receivedMessage;
                        try
                        {
                            receivedMessage = new FeederReceivedMessage<TKafkaFeederMessage>(message,
                                activityContext,
                                baggage,
                                new Dictionary<string, object?>
                                {
                                    { nameof(consumeResult.Topic), consumeResult.Topic },
                                    { nameof(consumeResult.Offset), consumeResult.Offset },
                                });

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
                    else
                        await Task.Yield();
                }
            }
            else
                await Task.Yield();
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
