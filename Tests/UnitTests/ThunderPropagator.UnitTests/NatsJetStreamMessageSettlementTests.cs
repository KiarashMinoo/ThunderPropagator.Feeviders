using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NSubstitute;
using ThunderPropagator.Feeders.NATS;

namespace ThunderPropagator.UnitTests
{
    public class NatsJetStreamMessageSettlementTests
    {
        [Fact]
        public async Task YieldAndSettleAsync_ShouldAckOnlyAfterProcessingAdvances()
        {
            var message = Substitute.For<INatsJSMsg<string>>();
            var logger = Substitute.For<ILogger>();
            await using var enumerator = NatsJetStreamMessageSettlement
                .YieldAndSettleAsync(message, "payload", logger, CancellationToken.None)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());
            Assert.Equal("payload", enumerator.Current);
            _ = message.DidNotReceiveWithAnyArgs().AckAsync(default, default);
            _ = message.DidNotReceiveWithAnyArgs().NakAsync(default, default);

            Assert.False(await enumerator.MoveNextAsync());
            await message.Received(1).AckAsync(cancellationToken: CancellationToken.None);
            _ = message.DidNotReceiveWithAnyArgs().NakAsync(default, default);
        }

        [Fact]
        public async Task YieldAndSettleAsync_ShouldNakWhenProcessingStopsEnumeration()
        {
            var message = Substitute.For<INatsJSMsg<string>>();
            var logger = Substitute.For<ILogger>();
            var enumerator = NatsJetStreamMessageSettlement
                .YieldAndSettleAsync(message, "payload", logger, CancellationToken.None)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());

            await enumerator.DisposeAsync();

            _ = message.DidNotReceiveWithAnyArgs().AckAsync(default, default);
            await message.Received(1).NakAsync(cancellationToken: CancellationToken.None);
        }

        [Fact]
        public async Task YieldAndSettleAsync_ShouldNakWhenAckFails()
        {
            var message = Substitute.For<INatsJSMsg<string>>();
            var logger = Substitute.For<ILogger>();
            message.AckAsync(cancellationToken: CancellationToken.None)
                .Returns(ValueTask.FromException(new InvalidOperationException("ack failed")));
            await using var enumerator = NatsJetStreamMessageSettlement
                .YieldAndSettleAsync(message, "payload", logger, CancellationToken.None)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await enumerator.MoveNextAsync().AsTask());
            await message.Received(1).NakAsync(cancellationToken: CancellationToken.None);
        }
    }
}
