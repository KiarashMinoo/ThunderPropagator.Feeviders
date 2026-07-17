using DotPulsar;
using DotPulsar.Abstractions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using ThunderPropagator.Feeders.Pulsar;

namespace ThunderPropagator.UnitTests
{
    public class PulsarMessageSettlementTests
    {
        private static readonly MessageId MessageId = new(1, 2, -1, -1, "test-topic");

        [Fact]
        public async Task YieldAndSettleAsync_ShouldAcknowledgeOnlyAfterProcessingAdvances()
        {
            var consumer = Substitute.For<IConsumer<string>>();
            var message = Substitute.For<IMessage<string>>();
            message.MessageId.Returns(MessageId);
            var logger = Substitute.For<ILogger>();

            await using var enumerator = PulsarMessageSettlement
                .YieldAndSettleAsync(consumer, message, "payload", logger, CancellationToken.None)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());
            Assert.Equal("payload", enumerator.Current);
            _ = consumer.DidNotReceiveWithAnyArgs().Acknowledge(default(MessageId)!, default);
            _ = consumer.DidNotReceiveWithAnyArgs().RedeliverUnacknowledgedMessages(default!, default);

            Assert.False(await enumerator.MoveNextAsync());
            await consumer.Received(1).Acknowledge(MessageId, CancellationToken.None);
            _ = consumer.DidNotReceiveWithAnyArgs().RedeliverUnacknowledgedMessages(default!, default);
        }

        [Fact]
        public async Task YieldAndSettleAsync_ShouldRedeliverWhenProcessingStopsEnumeration()
        {
            var consumer = Substitute.For<IConsumer<string>>();
            var message = Substitute.For<IMessage<string>>();
            message.MessageId.Returns(MessageId);
            var logger = Substitute.For<ILogger>();
            using var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;

            var enumerator = PulsarMessageSettlement
                .YieldAndSettleAsync(consumer, message, "payload", logger, cancellationToken)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());

            await enumerator.DisposeAsync();

            _ = consumer.DidNotReceiveWithAnyArgs().Acknowledge(default(MessageId)!, default);
            await consumer.Received(1).RedeliverUnacknowledgedMessages(
                Arg.Is<IEnumerable<MessageId>>(ids => ids!.SequenceEqual(new[] { MessageId })),
                cancellationToken);
        }

        [Fact]
        public async Task YieldAndSettleAsync_ShouldRedeliverWhenAcknowledgeFails()
        {
            var consumer = Substitute.For<IConsumer<string>>();
            var message = Substitute.For<IMessage<string>>();
            message.MessageId.Returns(MessageId);
            var logger = Substitute.For<ILogger>();
            using var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;
            consumer.Acknowledge(MessageId, cancellationToken)
                .Returns(ValueTask.FromException(new InvalidOperationException("acknowledge failed")));

            await using var enumerator = PulsarMessageSettlement
                .YieldAndSettleAsync(consumer, message, "payload", logger, cancellationToken)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await enumerator.MoveNextAsync().AsTask());
            await consumer.Received(1).RedeliverUnacknowledgedMessages(
                Arg.Is<IEnumerable<MessageId>>(ids => ids!.SequenceEqual(new[] { MessageId })),
                cancellationToken);
        }
    }
}
