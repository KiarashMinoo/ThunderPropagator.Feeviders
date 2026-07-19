using System.Reflection;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Feeders.Kafka;

namespace ThunderPropagator.UnitTests.IsEnabledGuards
{
    public class KafkaFeederIsEnabledTests
    {
        [Fact]
        public void Constructor_ShouldSkipConsumerCreation_WhenDisabled()
        {
            var configuration = new TestKafkaFeederConfiguration
            {
                IsEnabled = false,
                BootstrapServers = "127.0.0.1:1",
                GroupId = "test-group",
                TopicNames = ["test-topic"]
            };

            var feeder = new KafkaFeeder<IChannel, TestKafkaFeederMessage, TestKafkaFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                configuration,
                FeederTestHarness.CreateHandler<IChannel, TestKafkaFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestKafkaFeederMessage, TestKafkaFeederConfiguration>());

            Assert.Null(GetConsumerField(feeder));
        }

        [Fact]
        public void Constructor_ShouldCreateConsumer_WhenEnabled()
        {
            var configuration = new TestKafkaFeederConfiguration
            {
                IsEnabled = true,
                BootstrapServers = "127.0.0.1:1",
                GroupId = "test-group",
                TopicNames = ["test-topic"]
            };

            var feeder = new KafkaFeeder<IChannel, TestKafkaFeederMessage, TestKafkaFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                configuration,
                FeederTestHarness.CreateHandler<IChannel, TestKafkaFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestKafkaFeederMessage, TestKafkaFeederConfiguration>());

            Assert.NotNull(GetConsumerField(feeder));
        }

        private static object? GetConsumerField(object feeder)
            => feeder.GetType().GetField("_consumer", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(feeder);

        internal sealed class TestKafkaFeederMessage : KafkaFeederMessage;

        internal sealed class TestKafkaFeederConfiguration : KafkaFeederConfiguration;
    }
}
