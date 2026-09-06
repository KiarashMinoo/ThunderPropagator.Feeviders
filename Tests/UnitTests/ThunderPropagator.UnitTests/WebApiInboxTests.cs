using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeders.WebApi;

namespace ThunderPropagator.UnitTests
{
    public class WebApiInboxTests
    {
        [Fact]
        public void Inbox_ShouldDefaultToDisabled()
        {
            var configuration = new TestWebApiFeederConfiguration();

            Assert.False(configuration.Inbox.InboxEnabled);
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedByAnotherOwner_ShouldNotInvokeTheHandler()
        {
            var channelKey = Guid.NewGuid();
            var feederId = Guid.NewGuid();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.ClaimedByAnotherOwner());
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("webapi-store", InboxStoreType.InMemory).Returns(store);
            var options = new InboxOptions { InboxEnabled = true, StoreType = InboxStoreType.InMemory, StoreConnectionName = "webapi-store" };
            var coordinator = new InboxReceiveCoordinator(options, channelKey, feederId, storeFactory);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                "payload"u8.ToArray(), "text/plain; charset=utf-8", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.InProgress, outcome);
            Assert.False(handlerInvoked);
        }

        private class TestWebApiFeederConfiguration : WebApiFeederConfiguration;
    }
}
