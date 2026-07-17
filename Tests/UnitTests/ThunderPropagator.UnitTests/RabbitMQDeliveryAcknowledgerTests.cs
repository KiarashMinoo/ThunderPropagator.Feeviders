using NSubstitute;
using RabbitMQ.Client;
using ThunderPropagator.Feeders.RabbitMQ;

namespace ThunderPropagator.UnitTests
{
    public class RabbitMQDeliveryAcknowledgerTests
    {
        [Fact]
        public async Task AcknowledgeAsync_ShouldAckManualDelivery()
        {
            const ulong deliveryTag = 42;
            var channel = Substitute.For<IChannel>();

            await RabbitMQDeliveryAcknowledger.AcknowledgeAsync(
                channel,
                deliveryTag,
                false,
                CancellationToken.None);

            await channel.Received(1).BasicAckAsync(
                deliveryTag,
                false,
                CancellationToken.None);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task NegativeAcknowledgeAsync_ShouldNackWithConfiguredRequeue(bool requeue)
        {
            const ulong deliveryTag = 42;
            var channel = Substitute.For<IChannel>();

            await RabbitMQDeliveryAcknowledger.NegativeAcknowledgeAsync(
                channel,
                deliveryTag,
                false,
                requeue,
                CancellationToken.None);

            await channel.Received(1).BasicNackAsync(
                deliveryTag,
                false,
                requeue,
                CancellationToken.None);
        }

        [Fact]
        public async Task Settlement_ShouldDoNothingWhenAutoAckIsEnabled()
        {
            var channel = Substitute.For<IChannel>();

            await RabbitMQDeliveryAcknowledger.AcknowledgeAsync(
                channel,
                42,
                true,
                CancellationToken.None);
            await RabbitMQDeliveryAcknowledger.NegativeAcknowledgeAsync(
                channel,
                42,
                true,
                false,
                CancellationToken.None);

            _ = channel.DidNotReceiveWithAnyArgs().BasicAckAsync(default, default, default);
            _ = channel.DidNotReceiveWithAnyArgs().BasicNackAsync(default, default, default, default);
        }
    }
}
