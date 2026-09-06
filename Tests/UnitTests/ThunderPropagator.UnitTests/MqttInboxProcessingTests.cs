using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Mqtt;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    /// <summary>
    /// Mqtt has no broker-level acknowledgement exposed in this codebase (MQTTnet auto-PUBACKs
    /// internally), so unlike RabbitMQ there is no ack-ordering to verify here - only that
    /// <see cref="MqttFeederConfiguration.Inbox"/> binds disabled by default and that a duplicate/
    /// in-progress claim outcome never invokes the handler, which <see cref="InboxReceiveCoordinator"/>
    /// itself already guarantees generically (see InboxReceiveCoordinatorTests).
    /// </summary>
    public class MqttInboxProcessingTests
    {
        private static readonly byte[] Payload = "payload"u8.ToArray();
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();

        [Fact]
        public void Inbox_ShouldBeDisabledByDefault()
        {
            var configuration = new TestMqttFeederConfiguration();

            Assert.False(configuration.Inbox.InboxEnabled);
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedByAnotherOwner_ShouldNeverInvokeTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.ClaimedByAnotherOwner());
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("mqtt-store", InboxStoreType.InMemory).Returns(store);
            var options = new InboxOptions { InboxEnabled = true, StoreType = InboxStoreType.InMemory, StoreConnectionName = "mqtt-store" };
            var coordinator = new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.InProgress, outcome);
            Assert.False(handlerInvoked);
        }

        private sealed class TestMqttFeederConfiguration : MqttFeederConfiguration;
    }
}
