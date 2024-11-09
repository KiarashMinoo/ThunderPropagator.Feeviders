using RapidStreamer.Feeviders.RabbitMQ.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;
using RapidStreamer.BuildingBlocks.Application.Serializations;

namespace RapidStreamer.Providers.DotNet.RabbitMQ
{
    public abstract class RabbitMQProviderConfiguration : RabbitMQFeeviderConfiguration, IAbstractProviderConfiguration
    {
        public SerializerType SerializerType
        {
            get => Get(SerializerType.Json);
            set => Set(value);
        }
    }
}