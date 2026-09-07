using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.Outbox;
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

        public Guid Id
        {
            get => Get(Guid.NewGuid());
            set => Set(value);
        }

        /// <summary>Opt-in transactional Outbox configuration for this Provider. Disabled by default.</summary>
        public OutboxOptions Outbox
        {
            get => Get(new OutboxOptions());
            set => Set(value);
        }
    }
}
