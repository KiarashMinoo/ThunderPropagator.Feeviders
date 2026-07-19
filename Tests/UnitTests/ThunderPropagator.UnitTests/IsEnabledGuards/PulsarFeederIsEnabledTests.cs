using System.Reflection;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Feeders.Pulsar;

namespace ThunderPropagator.UnitTests.IsEnabledGuards
{
    public class PulsarFeederIsEnabledTests
    {
        [Fact]
        public void Constructor_ShouldSkipClientCreation_WhenDisabled()
        {
            var configuration = new TestPulsarFeederConfiguration
            {
                IsEnabled = false,
                ServiceUrl = new Uri("pulsar://127.0.0.1:6650"),
                SubscriptionName = "test-sub",
                Topic = "test-topic"
            };

            var feeder = new PulsarFeeder<IChannel, TestPulsarFeederMessage, TestPulsarFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                configuration,
                FeederTestHarness.CreateHandler<IChannel, TestPulsarFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestPulsarFeederMessage, TestPulsarFeederConfiguration>());

            Assert.Null(GetField(feeder, "_client"));
            Assert.Null(GetField(feeder, "_consumer"));
        }

        [Fact]
        public void Constructor_ShouldCreateClientAndConsumer_WhenEnabled()
        {
            var configuration = new TestPulsarFeederConfiguration
            {
                IsEnabled = true,
                ServiceUrl = new Uri("pulsar://127.0.0.1:6650"),
                SubscriptionName = "test-sub",
                Topic = "test-topic"
            };

            var feeder = new PulsarFeeder<IChannel, TestPulsarFeederMessage, TestPulsarFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                configuration,
                FeederTestHarness.CreateHandler<IChannel, TestPulsarFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestPulsarFeederMessage, TestPulsarFeederConfiguration>());

            Assert.NotNull(GetField(feeder, "_client"));
            Assert.NotNull(GetField(feeder, "_consumer"));
        }

        private static object? GetField(object feeder, string name)
            => feeder.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(feeder);

        internal sealed class TestPulsarFeederMessage : PulsarFeederMessage;

        internal sealed class TestPulsarFeederConfiguration : PulsarFeederConfiguration;
    }
}
