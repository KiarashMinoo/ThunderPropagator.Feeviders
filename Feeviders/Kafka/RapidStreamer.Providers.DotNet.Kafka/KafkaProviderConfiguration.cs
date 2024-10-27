using Confluent.Kafka;
using RapidStreamer.BuildingBlocks.Application.Serializations;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.Kafka
{
    public abstract class KafkaProviderConfiguration : ProducerConfig,
        IAbstractProviderConfiguration
    {
        public required string TopicName
        {
            get => Get("topic.name")!;
            set => Set("topic.name", value);
        }

        public string? SchemaRegistryUrl
        {
            get => Get("schema.registry.url");
            set => Set("schema.registry.url", value);
        }

        SerializerType IAbstractProviderConfiguration.SerializerType
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

        protected KafkaProviderConfiguration()
        {
        }

        protected KafkaProviderConfiguration(IEnumerable<KeyValuePair<string, string>> properties) : base(new Dictionary<string, string>(properties))
        {
        }

        public new void Set(string key, string? val) => base.Set($"-{key}", val);
        public new string? Get(string key) => base.Get($"-{key}");
        protected new int? GetInt(string key) => base.GetInt($"-{key}");
        protected new bool? GetBool(string key) => base.GetBool($"-{key}");
        protected new double? GetDouble(string key) => base.GetDouble($"-{key}");
        protected new object? GetEnum(Type type, string key) => base.GetEnum(type, $"-{key}");
        protected new void SetObject(string name, object? val) => base.SetObject($"-{name}", val);

        public ProducerConfig ToProducerConfig()
        {
            var kafkaConfig = this.Where(x => !x.Key.StartsWith('-')).ToDictionary(x => x.Key, x => x.Value);

            var producerConfig = new ProducerConfig(kafkaConfig);
            return producerConfig;
        }
    }
}