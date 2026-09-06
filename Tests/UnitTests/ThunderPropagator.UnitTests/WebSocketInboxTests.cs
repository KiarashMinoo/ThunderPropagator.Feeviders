using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeders.WebSocket;

namespace ThunderPropagator.UnitTests
{
    public class WebSocketInboxTests
    {
        [Fact]
        public void Inbox_ShouldDefaultToDisabled()
        {
            var configuration = new TestWebSocketFeederConfiguration();

            Assert.False(configuration.Inbox.InboxEnabled);
        }

        [Fact]
        public async Task ReceiveAsync_AlreadyProcessed_ShouldNotInvokeTheHandler()
        {
            var channelKey = Guid.NewGuid();
            var feederId = Guid.NewGuid();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.AlreadyProcessed());
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("websocket-store", InboxStoreType.InMemory).Returns(store);
            var options = new InboxOptions { InboxEnabled = true, StoreType = InboxStoreType.InMemory, StoreConnectionName = "websocket-store" };
            var coordinator = new InboxReceiveCoordinator(options, channelKey, feederId, storeFactory);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                "payload"u8.ToArray(), "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.Duplicate, outcome);
            Assert.False(handlerInvoked);
        }

        private class TestWebSocketFeederConfiguration : WebSocketFeederConfiguration;
    }
}
