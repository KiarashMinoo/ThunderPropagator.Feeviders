using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.RabbitMQ.SharedKernel;
using RapidStreamer.BuildingBlocks.Application.Serializations;

namespace RapidStreamer.Feeders.RabbitMQ
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