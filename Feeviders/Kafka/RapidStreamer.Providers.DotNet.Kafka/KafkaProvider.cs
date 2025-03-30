using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;
using RapidStreamer.BuildingBlocks.Application.Serializations;
using RapidStreamer.Providers.DotNet.Kafka.KafkaSerializers;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.Kafka
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration> : AbstractProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>
        where TKafkaProviderMessage : KafkaProviderMessage
        where TKafkaProviderConfiguration : KafkaProviderConfiguration
    {
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
                .SetErrorHandler((_, e) => Logger.LogError("Error: {Reason}", e.Reason))
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
                    cancellationToken);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception, "error has occured while producing message to topic {Topic}.", _kafkaProviderConfiguration.TopicName);
                throw;
            }
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}