using System.Reflection;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeders.Kafka.KafkaDeserializers;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;

namespace ThunderPropagator.Feeders.Kafka
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration> : IterativeFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
        where TChannel : class, IChannel
        where TKafkaFeederMessage : KafkaFeederMessage
        where TKafkaFeederConfiguration : KafkaFeederConfiguration
    {
        private readonly IConsumer<string, TKafkaFeederMessage> _consumer;
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

            HealthName = $"feeder_{nameof(Kafka)}_{_kafkaFeederConfiguration.GroupId}_{string.Join("_", _kafkaFeederConfiguration.TopicNames.Select(topicName => topicName))}";
            HealthTags = [.. HealthTags, nameof(Kafka), .. _kafkaFeederConfiguration.TopicNames];

            var consumerConfig = _kafkaFeederConfiguration.ToConsumerConfig();

            _consumer = new ConsumerBuilder<string, TKafkaFeederMessage>(consumerConfig)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(_kafkaFeederConfiguration.SerializerType switch
                {
                    KafkaSerializerType.Json => new KafkaJsonDeserializer<TKafkaFeederMessage>(this).AsSyncOverAsync(),
                    KafkaSerializerType.NJson => new KafkaNJsonDeserializer<TKafkaFeederMessage>(this).AsSyncOverAsync(),
                    KafkaSerializerType.NetJson => new KafkaNetJsonDeserializer<TKafkaFeederMessage>(this).AsSyncOverAsync(),
                    KafkaSerializerType.SchemaJson => new JsonDeserializer<TKafkaFeederMessage>(SchemaRegistryClient).AsSyncOverAsync(),
                    KafkaSerializerType.Avro => new AvroDeserializer<TKafkaFeederMessage>(SchemaRegistryClient).AsSyncOverAsync(),
                    _ => throw new ArgumentOutOfRangeException()
                })
                .SetErrorHandler((_, e) =>
                {
                    ReportHealth(HealthStatus.Unhealthy, new KafkaException(e));
                    Logger.LogError("Error: {Reason}", e.Reason);
                })
                .Build();

            _consumer.Subscribe(_kafkaFeederConfiguration.TopicNames);

            Logger.LogInformation(
                "{FeederName}/{ChannelName} on topic(s) {TopicNames} has subscribed.",
                GetType().Name,
                channel.Metadata.ChannelName,
                _kafkaFeederConfiguration.TopicNames);
        }

        protected override async IAsyncEnumerable<FeederReceivedMessage<TKafkaFeederMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var consumeResult = await BlockingOperationRunner.RunAsync(
                () => _consumer.Consume(cancellationToken),
                cancellationToken).ConfigureAwait(false);

            if (consumeResult is not null)
            {
                if (consumeResult.IsPartitionEOF)
                {
                    Logger.LogInformation("Reached end of topic {Topic}, partition {Partition}, offset {consumeResult.Offset}.", consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

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

                        yield return new FeederReceivedMessage<TKafkaFeederMessage>(message,
                            activityContext,
                            baggage,
                            new Dictionary<string, object?>
                            {
                                { nameof(consumeResult.Topic), consumeResult.Topic },
                                { nameof(consumeResult.Offset), consumeResult.Offset },
                            });
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

                    object topicNames = _kafkaFeederConfiguration.TopicNames;
                    Logger.LogError(kafkaException, "error has occured while consuming messages on topics {Topics}, Error = {Error}.", topicNames, kafkaException.Error);
                    break;
                }
                default:
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Logger.LogError(exception, "error has occured while consuming messages on topics {Topics}.", (object)_kafkaFeederConfiguration.TopicNames);
                    break;
            }

            await Task.Delay(TimeSpan.FromSeconds(awaitness), cancellationToken).ConfigureAwait(false);
            return true;
        }

        protected override Task StoppingAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                _consumer.Close();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while closing Kafka consumer.");
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
                Logger.LogWarning(ex, "Exception while disposing Kafka consumer.");
            }

            try
            {
                _schemaRegistry?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing schema registry.");
            }
        }
    }
}
