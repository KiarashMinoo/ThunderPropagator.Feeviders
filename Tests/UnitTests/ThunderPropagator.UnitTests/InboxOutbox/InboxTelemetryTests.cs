using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    // ChannelKey/FeederId are per-instance (xUnit creates a fresh instance per test method), never
    // static: MetricsCapture listens process-wide by meter name, so other test classes exercising the
    // same Inbox meter concurrently would otherwise contaminate these assertions - scoping every
    // assertion to this test's own unique channel key is what keeps that noise out.
    public class InboxTelemetryTests
    {
        private static readonly byte[] Payload = "payload"u8.ToArray();
        private readonly Guid ChannelKey = Guid.NewGuid();
        private readonly Guid FeederId = Guid.NewGuid();
        private const string MeterName = "thunderpropagator.feeders.inbox";

        [Fact]
        public async Task ReceiveAsync_NewClaim_ShouldIncrementReceivedTaggedWithTheChannel()
        {
            using var capture = new MetricsCapture(MeterName);
            var claimed = BuildClaimedMessage();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(InboxClaimResult.Claimed(claimed));
            var coordinator = BuildCoordinator(EnabledOptions(), store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => ValueTask.CompletedTask, _ => ValueTask.CompletedTask);

            var measurement = Assert.Single(ForThisChannel(capture, "thunderpropagator.feeders.inbox.received"));
            Assert.Equal(1, measurement.Value);
        }

        [Theory]
        [InlineData(InboxClaimOutcome.AlreadyProcessed)]
        [InlineData(InboxClaimOutcome.DeadLettered)]
        public async Task ReceiveAsync_RedeliveryOfATerminalEntry_ShouldIncrementDuplicates(InboxClaimOutcome outcome)
        {
            using var capture = new MetricsCapture(MeterName);
            var store = Substitute.For<IInboxStore>();
            var result = outcome == InboxClaimOutcome.AlreadyProcessed
                ? InboxClaimResult.AlreadyProcessed(BuildClaimedMessage())
                : InboxClaimResult.DeadLettered(BuildClaimedMessage());
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(result);
            var coordinator = BuildCoordinator(EnabledOptions(), store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => ValueTask.CompletedTask, _ => ValueTask.CompletedTask);

            Assert.Single(ForThisChannel(capture, "thunderpropagator.feeders.inbox.duplicates"));
            Assert.Empty(ForThisChannel(capture, "thunderpropagator.feeders.inbox.received"));
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedByAnotherOwner_ShouldIncrementClaimContention()
        {
            using var capture = new MetricsCapture(MeterName);
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.ClaimedByAnotherOwner(BuildClaimedMessage()));
            var coordinator = BuildCoordinator(EnabledOptions(), store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => ValueTask.CompletedTask, _ => ValueTask.CompletedTask);

            Assert.Single(ForThisChannel(capture, "thunderpropagator.feeders.inbox.claim_contention"));
        }

        [Fact]
        public async Task ReceiveAsync_HandlerSucceeds_ShouldIncrementProcessed()
        {
            using var capture = new MetricsCapture(MeterName);
            var claimed = BuildClaimedMessage();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(InboxClaimResult.Claimed(claimed));
            var coordinator = BuildCoordinator(EnabledOptions(), store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => ValueTask.CompletedTask, _ => ValueTask.CompletedTask);

            Assert.Single(ForThisChannel(capture, "thunderpropagator.feeders.inbox.processed"));
        }

        [Fact]
        public async Task ReceiveAsync_HandlerThrows_BelowMaxAttempts_ShouldIncrementFailedAndRecordRetryDelay()
        {
            using var capture = new MetricsCapture(MeterName);
            var claimed = BuildClaimedMessage(attemptCount: 1);
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(InboxClaimResult.Claimed(claimed));
            var options = EnabledOptions() with { MaxRetryAttempts = 5 };
            var coordinator = BuildCoordinator(options, store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => throw new InvalidOperationException("boom"), _ => ValueTask.CompletedTask);

            Assert.Single(ForThisChannel(capture, "thunderpropagator.feeders.inbox.failed"));
            Assert.Single(ForThisChannel(capture, "thunderpropagator.feeders.inbox.retry_delay"));
        }

        [Fact]
        public async Task ReceiveAsync_HandlerThrows_AtMaxAttempts_ShouldIncrementDeadLetteredTaggedWithCategory()
        {
            using var capture = new MetricsCapture(MeterName);
            var claimed = BuildClaimedMessage(attemptCount: 1);
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(InboxClaimResult.Claimed(claimed));
            store.DeadLetterAsync(claimed.Id, Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(claimed.TryTransitionTo(InboxMessageStatus.DeadLettered, TimeProvider.System, failureReason: "boom"));
            var options = EnabledOptions() with { MaxRetryAttempts = 1 };
            var coordinator = BuildCoordinator(options, store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => throw new InvalidOperationException("boom"), _ => ValueTask.CompletedTask);

            var measurement = Assert.Single(ForThisChannel(capture, "thunderpropagator.feeders.inbox.dead_lettered"));
            Assert.Equal(InboxDeadLetterFailureCategory.Poison.ToString(), measurement.Tags["category"]);
        }

        [Fact]
        public async Task ReceiveAsync_ShouldRecordStoreOperationDurationForEveryStoreCall()
        {
            using var capture = new MetricsCapture(MeterName);
            var claimed = BuildClaimedMessage();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(InboxClaimResult.Claimed(claimed));
            var coordinator = BuildCoordinator(EnabledOptions(), store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => ValueTask.CompletedTask, _ => ValueTask.CompletedTask);

            var durations = ForThisChannel(capture, "thunderpropagator.feeders.inbox.store_operation.duration");
            Assert.Contains(durations, m => Equals(m.Tags["operation"], "TryClaim") && Equals(m.Tags["outcome"], "success"));
            Assert.Contains(durations, m => Equals(m.Tags["operation"], "Complete") && Equals(m.Tags["outcome"], "success"));
        }

        private List<(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags)> ForThisChannel(MetricsCapture capture, string instrument) =>
            capture.Measurements.Where(m => m.Instrument == instrument && Equals(m.Tags.GetValueOrDefault("channel"), ChannelKey)).ToList();

        private InboxReceiveCoordinator BuildCoordinator(InboxOptions options, IInboxStore store)
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            if (options.InboxEnabled)
                storeFactory.GetStore(options.StoreConnectionName!, options.StoreType).Returns(store);

            return new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
        }

        private static InboxOptions EnabledOptions() => new()
        {
            InboxEnabled = true,
            StoreType = InboxStoreType.InMemory,
            StoreConnectionName = "test-store",
        };

        private InboxMessage BuildClaimedMessage(int attemptCount = 1) =>
            InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-id", ChannelKey, FeederId,
                schemaVersion: 1, payloadContentType: "application/octet-stream", payload: Payload,
                headers: null, partitionKey: null, timeProvider: TimeProvider.System)
            .TryTransitionTo(InboxMessageStatus.Processing, TimeProvider.System, leaseOwner: "owner", leaseExpiresAtUtc: DateTimeOffset.UtcNow.AddSeconds(30), incrementAttempt: true)
            with
            { AttemptCount = attemptCount };
    }
}
