using NSubstitute;
using RabbitMQ.Client;
using ThunderPropagator.Providers.DotNet.RabbitMQ;

namespace ThunderPropagator.UnitTests
{
    public class RabbitMQProviderChannelTests
    {
        [Fact]
        public void GetReadyChannel_ShouldRejectMissingChannel()
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                RabbitMQProvider<RabbitMQProviderMessage, TestRabbitMQProviderConfiguration>
                    .GetReadyChannel(null, "orders"));

            Assert.Contains("orders", exception.Message);
            Assert.Contains("not ready", exception.Message);
            Assert.Contains("not published", exception.Message);
        }

        [Fact]
        public void GetReadyChannel_ShouldRejectClosedChannel()
        {
            var channel = Substitute.For<IChannel>();
            channel.IsOpen.Returns(false);

            Assert.Throws<InvalidOperationException>(() =>
                RabbitMQProvider<RabbitMQProviderMessage, TestRabbitMQProviderConfiguration>
                    .GetReadyChannel(channel, "orders"));
        }

        [Fact]
        public void GetReadyChannel_ShouldReturnOpenChannel()
        {
            var channel = Substitute.For<IChannel>();
            channel.IsOpen.Returns(true);

            var readyChannel = RabbitMQProvider<RabbitMQProviderMessage, TestRabbitMQProviderConfiguration>
                .GetReadyChannel(channel, "orders");

            Assert.Same(channel, readyChannel);
        }

        private sealed class TestRabbitMQProviderConfiguration : RabbitMQProviderConfiguration;
    }
}
