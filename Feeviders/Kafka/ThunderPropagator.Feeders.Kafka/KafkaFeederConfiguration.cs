using Confluent.Kafka;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Feeders.Kafka
{
    public abstract class KafkaFeederConfiguration : ConsumerConfig,
        IAbstractFeederConfiguration
    {
        public bool IsEnabled
        {
            get => GetBool("enabled") ?? false;
            set => Set("enabled", value ? "true" : "false");
        }

        public Guid Id
        {
            get => Guid.TryParse(Get("id"), out var id) ? id : Guid.NewGuid();
            set => Set("id", value.ToString());
        }

        public string[] TopicNames
        {
            get => Get("topic.names")?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) ?? [];
            set => Set("topic.names", string.Join(',', value));
        }

        public string? SchemaRegistryUrl
        {
            get => Get("schema.registry.url");
            set => Set("schema.registry.url", value);
        }

        SerializerType IAbstractFeederConfiguration.SerializerType
        {
            get => (SerializerType)SerializerType;
            set => SerializerType = (KafkaSerializerType)value;
        }

        public KafkaSerializerType SerializerType
        {
            get
            {
                var @enum = GetEnum(typeof(KafkaSerializerType), "serializer.type");
                return @enum is not null ? (KafkaSerializerType)@enum : KafkaSerializerType.Json;
            }
            set => SetObject("serializer.type", value);
        }

        public string? EnrichmentScript
        {
            get => Get("enrichment.script");
            set => Set("enrichment.script", value);
        }

        public string[]? MetadataReferences
        {
            get => Get("metadata.references")?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) ?? [];
            set
            {
                if (value is not null)
                    Set("metadata.references", string.Join(',', value));
            }
        }

        protected KafkaFeederConfiguration()
        {
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Latest;
        }

        protected KafkaFeederConfiguration(IEnumerable<KeyValuePair<string, string>> properties) : base(new Dictionary<string, string>(properties))
        {
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Latest;
        }

        public new void Set(string key, string? val) => base.Set($"-{key}", val);
        public new string? Get(string key) => base.Get($"-{key}");
        protected new int? GetInt(string key) => base.GetInt($"-{key}");
        protected new bool? GetBool(string key) => base.GetBool($"-{key}");
        protected new double? GetDouble(string key) => base.GetDouble($"-{key}");
        protected new object? GetEnum(Type type, string key) => base.GetEnum(type, $"-{key}");
        protected new void SetObject(string name, object? val) => base.SetObject($"-{name}", val);


        public ConsumerConfig ToConsumerConfig()
        {
            var kafkaConfig = this.Where(x => !x.Key.StartsWith('-')).ToDictionary(x => x.Key, x => x.Value);

            return new ConsumerConfig(kafkaConfig);
        }

        public ProducerConfig ToProducerConfig()
        {
            var ignores = new[] { "group.id", "session.timeout.ms", "enable.auto.commit", "enable.auto.offset.store", "auto.offset.reset" };

            var consumerConfig = ToConsumerConfig();

            var kafkaConfig = consumerConfig.Where(x => !ignores.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value);

            return new ProducerConfig(kafkaConfig);
        }
    }
}