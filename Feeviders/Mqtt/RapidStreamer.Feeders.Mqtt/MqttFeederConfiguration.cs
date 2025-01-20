using RapidStreamer.Application.Feeders;
using RapidStreamer.BuildingBlocks.Application.Serializations;
using RapidStreamer.Feeviders.Mqtt.SharedKernel;

namespace RapidStreamer.Feeders.Mqtt
{
    public abstract class MqttFeederConfiguration : MqttFeeviderConfiguration, IAbstractFeederConfiguration
    {
        public Guid Id
        {
            get => Get(Guid.NewGuid());
            set => Set(value);
        }

        public SerializerType SerializerType
        {
            get => Get(SerializerType.Json);
            set => Set(value);
        }

        public string? EnrichmentScript
        {
            get => Get<string>();
            set => Set(value);
        }

        public string[]? MetadataReferences
        {
            get => Get<string[]>();
            set => Set(value);
        }
    }
}