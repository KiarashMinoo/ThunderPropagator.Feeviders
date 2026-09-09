using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Feeders.Kafka
{
    public abstract class KafkaFeederMessage : FeederMessage
    {
        private const string RawPayloadKey = "__kafka_inbox_raw_payload";

        /// <summary>
        /// The exact wire bytes this message was deserialized from. Kafka's client deserializes
        /// inline during <c>IConsumer.Consume()</c>, so - unlike RabbitMQ, which hands over raw bytes
        /// untouched - the original bytes are otherwise gone by the time a
        /// <see cref="KafkaFeeder{TChannel,TKafkaFeederMessage,TKafkaFeederConfiguration}"/> sees the
        /// <c>ConsumeResult</c>. Set by <see cref="KafkaDeserializer{T}"/> immediately after a
        /// successful deserialize, so an Inbox-enabled feeder can persist the exact original payload
        /// alongside its durable claim.
        /// </summary>
        internal byte[]? RawPayload
        {
            get => GetValueOrNull<byte[]>(RawPayloadKey);
            set => SetValue(value, RawPayloadKey);
        }
    }
}
