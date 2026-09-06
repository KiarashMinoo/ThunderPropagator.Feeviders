using NSubstitute;
using RabbitMQ.Client;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.RabbitMQ;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    /// <summary>
    /// Exercises the same collaboration <see cref="RabbitMQFeeder{TChannel,TFeederMessage,TFeederConfiguration}"/>'s
    /// private receive handler wires together - <see cref="InboxReceiveCoordinator"/> driving
    /// <see cref="RabbitMQDeliveryAcknowledger"/> - without needing a live broker connection, which the
    /// handler's own private method (and the connection setup around it) requires end-to-end.
    /// </summary>
    public class RabbitMQInboxAcknowledgementTests
    {
        private const ulong DeliveryTag = 7;
        private static readonly byte[] Payload = "payload"u8.ToArray();
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();

        [Fact]
        public async Task Claimed_HandlerSucceeds_ShouldAckBeforeTheHandlerRunsAndNeverNack()
        {
            var amqpChannel = Substitute.For<IChannel>();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.Claimed(BuildClaimedMessage(attemptCount: 1)));
            var coordinator = BuildCoordinator(store);
            var sequence = new List<string>();

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { sequence.Add("handler"); return ValueTask.CompletedTask; },
                acknowledgeAsync: ct => Ack(amqpChannel, ct, sequence));

            await Dispatch(outcome, amqpChannel);

            Assert.Equal(InboxReceiveOutcome.Processed, outcome);
            Assert.Equal(["ack", "handler"], sequence);
            await amqpChannel.Received(1).BasicAckAsync(DeliveryTag, false, Arg.Any<CancellationToken>());
            await amqpChannel.DidNotReceiveWithAnyArgs().BasicNackAsync(default, default, default, default);
        }

        [Fact]
        public async Task Claimed_HandlerThrows_ShouldStillOnlyAckOnceAndNeverNack()
        {
            var amqpChannel = Substitute.For<IChannel>();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.Claimed(BuildClaimedMessage(attemptCount: 1)));
            var coordinator = BuildCoordinator(store);

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => throw new InvalidOperationException("handler failed"),
                acknowledgeAsync: ct => Ack(amqpChannel, ct, []));

            await Dispatch(outcome, amqpChannel);

            Assert.Equal(InboxReceiveOutcome.Failed, outcome);
            await amqpChannel.Received(1).BasicAckAsync(DeliveryTag, false, Arg.Any<CancellationToken>());
            await amqpChannel.DidNotReceiveWithAnyArgs().BasicNackAsync(default, default, default, default);
        }

        [Fact]
        public async Task Duplicate_ShouldAckWithoutInvokingTheHandler()
        {
            var amqpChannel = Substitute.For<IChannel>();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.AlreadyProcessed());
            var coordinator = BuildCoordinator(store);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: ct => Ack(amqpChannel, ct, []));

            await Dispatch(outcome, amqpChannel);

            Assert.False(handlerInvoked);
            await amqpChannel.Received(1).BasicAckAsync(DeliveryTag, false, Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task InProgress_ShouldRequeueWithoutAckingOrInvokingTheHandler()
        {
            var amqpChannel = Substitute.For<IChannel>();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.ClaimedByAnotherOwner());
            var coordinator = BuildCoordinator(store);
            var handlerInvoked = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: ct => Ack(amqpChannel, ct, []));

            await Dispatch(outcome, amqpChannel);

            Assert.False(handlerInvoked);
            await amqpChannel.DidNotReceiveWithAnyArgs().BasicAckAsync(default, default, default);
            await amqpChannel.Received(1).BasicNackAsync(DeliveryTag, false, true, Arg.Any<CancellationToken>());
        }

        // Mirrors RabbitMQFeeder.HandleReceivedAsync's outcome switch for the delivery-acknowledgement
        // decision only (health/metrics reporting is exercised by RabbitMQFeeder itself, not here).
        private static async Task Dispatch(InboxReceiveOutcome outcome, IChannel amqpChannel)
        {
            switch (outcome)
            {
                case InboxReceiveOutcome.InProgress:
                    await RabbitMQDeliveryAcknowledger.NegativeAcknowledgeAsync(amqpChannel, DeliveryTag, false, true, CancellationToken.None);
                    break;
                case InboxReceiveOutcome.Disabled:
                    await RabbitMQDeliveryAcknowledger.AcknowledgeAsync(amqpChannel, DeliveryTag, false, CancellationToken.None);
                    break;
                // Processed/Duplicate/AlreadyDeadLettered/Failed/DeadLettered: already acknowledged
                // inside ReceiveAsync itself via the acknowledgeAsync delegate under test.
            }
        }

        private static ValueTask Ack(IChannel amqpChannel, CancellationToken cancellationToken, List<string> sequence)
        {
            sequence.Add("ack");
            return RabbitMQDeliveryAcknowledger.AcknowledgeAsync(amqpChannel, DeliveryTag, false, cancellationToken);
        }

        private static InboxReceiveCoordinator BuildCoordinator(IInboxStore store)
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("rabbitmq-store", InboxStoreType.InMemory).Returns(store);

            var options = new InboxOptions
            {
                InboxEnabled = true,
                StoreType = InboxStoreType.InMemory,
                StoreConnectionName = "rabbitmq-store",
            };

            return new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
        }

        private static InboxMessage BuildClaimedMessage(int attemptCount) =>
            InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-id", ChannelKey, FeederId,
                schemaVersion: 1, payloadContentType: "application/octet-stream", payload: Payload,
                headers: null, partitionKey: null, timeProvider: TimeProvider.System)
            .TryTransitionTo(InboxMessageStatus.Processing, TimeProvider.System, leaseOwner: "owner", leaseExpiresAtUtc: DateTimeOffset.UtcNow.AddSeconds(30), incrementAttempt: true)
            with
            { AttemptCount = attemptCount };
    }
}
