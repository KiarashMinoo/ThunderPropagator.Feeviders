using NSubstitute;
using ThunderPropagator.Feeders.ActiveMQ;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    /// <summary>
    /// This ActiveMQ/JMS consumer uses the session's implicit acknowledgement mode with no explicit
    /// <c>message.Acknowledge()</c> call, so unlike RabbitMQ there is no ack-ordering to verify here -
    /// only that <see cref="ActiveMQFeederConfiguration.Inbox"/> binds disabled by default and that a
    /// duplicate/already-processed claim outcome never invokes the handler, which
    /// <see cref="InboxReceiveCoordinator"/> itself already guarantees generically (see
    /// InboxReceiveCoordinatorTests).
    /// </summary>
    public class ActiveMQInboxProcessingTests
    {
        private static readonly byte[] Payload = "payload"u8.ToArray();
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();

        [Fact]
        public void Inbox_ShouldBeDisabledByDefault()
        {
            var configuration = new TestActiveMQFeederConfiguration();

            Assert.False(configuration.Inbox.InboxEnabled);
        }

        [Fact]
        public async Task ReceiveAsync_AlreadyProcessed_ShouldNeverInvokeTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.AlreadyProcessed());
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("activemq-store", InboxStoreType.InMemory).Returns(store);
            var options = new InboxOptions { InboxEnabled = true, StoreType = InboxStoreType.InMemory, StoreConnectionName = "activemq-store" };
            var coordinator = new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.Duplicate, outcome);
            Assert.False(handlerInvoked);
        }

        private sealed class TestActiveMQFeederConfiguration : ActiveMQFeederConfiguration;
    }
}
