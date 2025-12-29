using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Feeders.RabbitMQ
{
    public abstract class RabbitMQFeederConfiguration : RabbitMQFeeviderConfiguration, IAbstractFeederConfiguration
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