using MQTTnet;

namespace ThunderPropagator.Feeders.Mqtt
{
    internal static class MqttSubscriptionOptionsFactory
    {
        public static MqttClientSubscribeOptions Create(
            MqttClientFactory mqttFactory,
            MqttFeederConfiguration feederConfiguration)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(feederConfiguration.Topic);

            var builder = mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(feederConfiguration.Topic);

            if (feederConfiguration.SubscriptionIdentifier is not null)
                builder.WithSubscriptionIdentifier(feederConfiguration.SubscriptionIdentifier.Value);

            return builder.Build();
        }
    }
}
