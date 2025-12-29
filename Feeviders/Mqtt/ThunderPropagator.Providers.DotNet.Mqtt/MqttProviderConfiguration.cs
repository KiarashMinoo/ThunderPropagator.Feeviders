using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeviders.Mqtt.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Mqtt
{
    public abstract class MqttProviderConfiguration : MqttFeeviderConfiguration, IAbstractProviderConfiguration
    {
        public SerializerType SerializerType
        {
            get => Get(SerializerType.Json);
            set => Set(value);
        }
    }
}