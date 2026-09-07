using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Feeviders.Mqtt.SharedKernel;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Mqtt
{
    public abstract class MqttProviderConfiguration : MqttFeeviderConfiguration, IAbstractProviderConfiguration
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
