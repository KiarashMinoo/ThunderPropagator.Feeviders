using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Providers.DotNet.Kafka.KafkaSerializers;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Kafka
{
    internal
#if !DEBUG
        sealed
#endif
        partial class KafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration> : AbstractProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>
        where TKafkaProviderMessage : KafkaProviderMessage
        where TKafkaProviderConfiguration : KafkaProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4000, Level = LogLevel.Error, Message = "Error: {Reason}")]
            public static partial void ProduceError(ILogger logger, string reason);

            [LoggerMessage(EventId = 4001, Level = LogLevel.Error, Message = "error has occured while producing message to topic {Topic}.")]
            public static partial void ProduceException(ILogger logger, Exception exception, string topic);
        }

        private readonly TKafkaProviderConfiguration _kafkaProviderConfiguration;
        private readonly IProducer<string, TKafkaProviderMessage> _producer;
        private CachedSchemaRegistryClient? _schemaRegistry;

        public KafkaProvider(TKafkaProviderConfiguration kafkaProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _kafkaProviderConfiguration = kafkaProviderConfiguration;

            _producer = new ProducerBuilder<string, TKafkaProviderMessage>(_kafkaProviderConfiguration)
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(
                    _kafkaProviderConfiguration.SerializerType switch
                    {
                        KafkaSerializerType.Json => new KafkaJsonSerializer<TKafkaProviderMessage>(this).AsSyncOverAsync(),
                        KafkaSerializerType.NJson => new KafkaNJsonSerializer<TKafkaProviderMessage>(this).AsSyncOverAsync(),
                        KafkaSerializerType.NetJson => new KafkaNetJsonSerializer<TKafkaProviderMessage>(this).AsSyncOverAsync(),
                        KafkaSerializerType.SchemaJson => new JsonSerializer<TKafkaProviderMessage>(SchemaRegistryClient).AsSyncOverAsync(),
                        KafkaSerializerType.Avro => new AvroSerializer<TKafkaProviderMessage>(SchemaRegistryClient).AsSyncOverAsync(),
                        _ => throw new ArgumentOutOfRangeException()
                    })
                .SetErrorHandler((_, e) => Log.ProduceError(Logger, e.Reason))
                .Build();
        }

        private ISchemaRegistryClient SchemaRegistryClient
            => _schemaRegistry = _schemaRegistry switch
            {
                null when string.IsNullOrWhiteSpace(_kafkaProviderConfiguration.SchemaRegistryUrl) => throw new InvalidOperationException("SchemaRegistry Url is required"),
                null => new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = _kafkaProviderConfiguration.SchemaRegistryUrl }),
                _ => _schemaRegistry
            };

        protected override async Task InternalExecuteAsync(TKafkaProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            using var activity = KafkaProviderExtensions.ActivitySource.StartActivity("kafka publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.destination.name", _kafkaProviderConfiguration.TopicName);
            activity?.SetTag("messaging.operation", "publish");

            var publishTimestamp = Stopwatch.GetTimestamp();
            try
            {
                var message = new Message<string, TKafkaProviderMessage>
                {
                    Key = feederMessage.KafkaProviderKey,
                    Value = feederMessage
                };

                if (Activity.Current?.Context is not null)
                    message.Headers.Add(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

                message.Headers.Add(nameof(Baggage), Baggage.Current.ToNJsonBytes());

                await _producer.ProduceAsync(_kafkaProviderConfiguration.TopicName,
                    message,
                    cancellationToken).ConfigureAwait(false);

                KafkaProviderExtensions.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                KafkaProviderExtensions.MessagesPublishFailed.Add(1);
                Log.ProduceException(Logger, exception, _kafkaProviderConfiguration.TopicName);
                throw;
            }
            finally
            {
                KafkaProviderExtensions.PublishDuration.Record(Stopwatch.GetElapsedTime(publishTimestamp).TotalMilliseconds);
            }
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}