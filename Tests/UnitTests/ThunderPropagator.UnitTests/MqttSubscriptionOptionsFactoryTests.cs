using MQTTnet;
using ThunderPropagator.Feeders.Mqtt;

namespace ThunderPropagator.UnitTests
{
    public class MqttSubscriptionOptionsFactoryTests
    {
        [Fact]
        public void Create_ShouldAddConfiguredTopicFilterAndSubscriptionIdentifier()
        {
            const string topic = "channels/updates";
            const uint subscriptionIdentifier = 42;
            var configuration = new TestMqttFeederConfiguration
            {
                Topic = topic,
                SubscriptionIdentifier = subscriptionIdentifier
            };

            var options = MqttSubscriptionOptionsFactory.Create(new MqttClientFactory(), configuration);

            var topicFilter = Assert.Single(options.TopicFilters);
            Assert.Equal(topic, topicFilter.Topic);
            Assert.Equal(subscriptionIdentifier, options.SubscriptionIdentifier);
        }

        [Fact]
        public void Create_ShouldRejectMissingTopic()
        {
            var configuration = new TestMqttFeederConfiguration { Topic = string.Empty };

            var exception = Assert.ThrowsAny<ArgumentException>(() =>
                MqttSubscriptionOptionsFactory.Create(new MqttClientFactory(), configuration));

            Assert.Contains(nameof(configuration.Topic), exception.Message);
        }

        private sealed class TestMqttFeederConfiguration : MqttFeederConfiguration;
    }
}
