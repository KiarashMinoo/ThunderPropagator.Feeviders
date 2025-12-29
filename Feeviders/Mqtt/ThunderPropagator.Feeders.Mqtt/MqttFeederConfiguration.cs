using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeviders.Mqtt.SharedKernel;

namespace ThunderPropagator.Feeders.Mqtt
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