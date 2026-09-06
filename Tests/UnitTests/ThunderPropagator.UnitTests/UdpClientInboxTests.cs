using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeders.UdpClient;

namespace ThunderPropagator.UnitTests
{
    public class UdpClientInboxTests
    {
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();

        [Fact]
        public void Configuration_Default_ShouldHaveInboxDisabled()
        {
            var configuration = new TestUdpClientFeederConfiguration();

            Assert.False(configuration.Inbox.InboxEnabled);
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedByAnotherOwner_ShouldNotInvokeTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.ClaimedByAnotherOwner());
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("udpclient-store", InboxStoreType.InMemory).Returns(store);
            var options = new InboxOptions { InboxEnabled = true, StoreType = InboxStoreType.InMemory, StoreConnectionName = "udpclient-store" };
            var coordinator = new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                "payload"u8.ToArray(), "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.InProgress, outcome);
            Assert.False(handlerInvoked);
        }

        [Fact]
        public async Task ReceiveAsync_AlreadyProcessed_ShouldNotInvokeTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.AlreadyProcessed());
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("udpclient-store", InboxStoreType.InMemory).Returns(store);
            var options = new InboxOptions { InboxEnabled = true, StoreType = InboxStoreType.InMemory, StoreConnectionName = "udpclient-store" };
            var coordinator = new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                "payload"u8.ToArray(), "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.Duplicate, outcome);
            Assert.False(handlerInvoked);
        }

        private sealed class TestUdpClientFeederConfiguration : UdpClientFeederConfiguration;
    }
}
