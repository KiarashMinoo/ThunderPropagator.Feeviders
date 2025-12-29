using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Providers.DotNet.Kafka
{
    public abstract class KafkaProviderMessage : FeederMessage
    {
        protected KafkaProviderMessage(string key) => KafkaProviderKey = key;

        public string KafkaProviderKey { get; }
    }
}