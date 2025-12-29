using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Providers.DotNet.Mqtt
{
    public abstract class MqttProviderMessage : FeederMessage
    {
        protected MqttProviderMessage(string key) => MqttProviderKey = key;

        public string MqttProviderKey { get; }
    }
}