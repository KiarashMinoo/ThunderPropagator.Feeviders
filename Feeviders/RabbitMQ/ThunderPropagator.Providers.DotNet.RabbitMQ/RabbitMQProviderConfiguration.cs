using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Providers.DotNet.RabbitMQ
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