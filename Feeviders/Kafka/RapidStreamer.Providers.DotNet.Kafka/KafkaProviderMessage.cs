using RapidStreamer.BuildingBlocks.Application;

namespace RapidStreamer.Providers.DotNet.Kafka
{
    public abstract class KafkaProviderMessage : FeederMessage
    {
        protected KafkaProviderMessage(string key) => KafkaProviderKey = key;

        public string KafkaProviderKey { get; }
    }
}