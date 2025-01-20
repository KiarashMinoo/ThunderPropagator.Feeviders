using RapidStreamer.BuildingBlocks.Application;

namespace RapidStreamer.Providers.DotNet.Mqtt
{
    public abstract class MqttProviderMessage : FeederMessage
    {
        protected MqttProviderMessage(string key) => MqttProviderKey = key;

        public string MqttProviderKey { get; }
    }
}