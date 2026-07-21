using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;

namespace ThunderPropagator.Providers.DotNet.RabbitMQ
{
    public abstract class RabbitMQProviderConfiguration : RabbitMQFeeviderConfiguration, IAbstractProviderConfiguration
    {
        public SerializerType SerializerType
        {
            get => Get(JsonFormatSerializer.Json);
            set => Set(value);
        }
    }
}
