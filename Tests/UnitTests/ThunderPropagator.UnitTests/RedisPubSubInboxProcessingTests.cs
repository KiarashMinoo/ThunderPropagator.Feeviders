using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.RedisPubSub;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    /// <summary>
    /// Redis pub/sub is fire-and-forget with no redelivery/acknowledgement concept, so unlike RabbitMQ
    /// there is no ack-ordering to verify here - only that <see cref="RedisPubSubFeederConfiguration.Inbox"/>
    /// binds disabled by default and that a duplicate/already-dead-lettered claim outcome never invokes
    /// the handler, which <see cref="InboxReceiveCoordinator"/> itself already guarantees generically
    /// (see InboxReceiveCoordinatorTests).
    /// </summary>
    public class RedisPubSubInboxProcessingTests
    {
        private static readonly byte[] Payload = "payload"u8.ToArray();
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();

        [Fact]
        public void Inbox_ShouldBeDisabledByDefault()
        {
            var configuration = new TestRedisPubSubFeederConfiguration();

            Assert.False(configuration.Inbox.InboxEnabled);
        }

        [Fact]
        public async Task ReceiveAsync_AlreadyDeadLettered_ShouldNeverInvokeTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.DeadLettered());
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("redis-store", InboxStoreType.InMemory).Returns(store);
            var options = new InboxOptions { InboxEnabled = true, StoreType = InboxStoreType.InMemory, StoreConnectionName = "redis-store" };
            var coordinator = new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.AlreadyDeadLettered, outcome);
            Assert.False(handlerInvoked);
        }

        private sealed class TestRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration;
    }
}
