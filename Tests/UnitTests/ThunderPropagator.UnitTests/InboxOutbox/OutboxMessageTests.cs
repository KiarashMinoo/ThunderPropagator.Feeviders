using System.Text.Json;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxMessageTests
    {
        private static readonly OutboxMessageStatus[] AllStatuses =
        [
            OutboxMessageStatus.Pending,
            OutboxMessageStatus.Publishing,
            OutboxMessageStatus.Published,
            OutboxMessageStatus.Failed,
            OutboxMessageStatus.DeadLettered,
        ];

        private static readonly HashSet<(OutboxMessageStatus From, OutboxMessageStatus To)> AllowedTransitions =
        [
            (OutboxMessageStatus.Pending, OutboxMessageStatus.Publishing),
            (OutboxMessageStatus.Publishing, OutboxMessageStatus.Published),
            (OutboxMessageStatus.Publishing, OutboxMessageStatus.Failed),
            (OutboxMessageStatus.Publishing, OutboxMessageStatus.DeadLettered),
            (OutboxMessageStatus.Failed, OutboxMessageStatus.Publishing),
            (OutboxMessageStatus.Failed, OutboxMessageStatus.DeadLettered),
        ];

        public static TheoryData<OutboxMessageStatus, OutboxMessageStatus, bool> AllTransitionPairs()
        {
            var data = new TheoryData<OutboxMessageStatus, OutboxMessageStatus, bool>();

            foreach (var from in AllStatuses)
            foreach (var to in AllStatuses)
                data.Add(from, to, AllowedTransitions.Contains((from, to)));

            return data;
        }

        [Theory]
        [MemberData(nameof(AllTransitionPairs))]
        public void CanTransitionTo_ShouldMatchTheAllowedTransitionTable(OutboxMessageStatus from, OutboxMessageStatus to, bool expectedAllowed)
        {
            var message = CreateInState(from);

            Assert.Equal(expectedAllowed, message.CanTransitionTo(to));
        }

        [Theory]
        [MemberData(nameof(AllTransitionPairs))]
        public void TryTransitionTo_ShouldThrowOnlyForDisallowedTransitions(OutboxMessageStatus from, OutboxMessageStatus to, bool expectedAllowed)
        {
            var message = CreateInState(from);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);

            if (expectedAllowed)
            {
                var result = message.TryTransitionTo(to, timeProvider);
                Assert.Equal(to, result.Status);
            }
            else
            {
                var exception = Assert.Throws<OutboxMessageTransitionException>(() => message.TryTransitionTo(to, timeProvider));
                Assert.Equal(from, exception.From);
                Assert.Equal(to, exception.To);
            }
        }

        [Fact]
        public void TryTransitionTo_Publishing_ShouldStampLeaseAndIncrementAttemptsWhenRequested()
        {
            var timeProvider = new ManualTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
            var pending = CreateInState(OutboxMessageStatus.Pending);
            var leaseExpiresAtUtc = timeProvider.GetUtcNow().AddMinutes(5);

            var publishing = pending.TryTransitionTo(
                OutboxMessageStatus.Publishing,
                timeProvider,
                leaseOwner: "worker-1",
                leaseExpiresAtUtc: leaseExpiresAtUtc,
                incrementAttempt: true);

            Assert.Equal(OutboxMessageStatus.Publishing, publishing.Status);
            Assert.Equal(1, publishing.Attempts);
            Assert.Equal("worker-1", publishing.LeaseOwner);
            Assert.Equal(leaseExpiresAtUtc, publishing.LeaseExpiresAtUtc);
            Assert.Equal(timeProvider.GetUtcNow(), publishing.PublishingStartedAtUtc);
        }

        [Fact]
        public void TryTransitionTo_Published_ShouldClearLeaseAndStampPublishedAtUtc()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var publishing = CreateInState(OutboxMessageStatus.Publishing, leaseOwner: "worker-1");

            timeProvider.Advance(TimeSpan.FromSeconds(30));
            var published = publishing.TryTransitionTo(OutboxMessageStatus.Published, timeProvider);

            Assert.Equal(OutboxMessageStatus.Published, published.Status);
            Assert.Equal(timeProvider.GetUtcNow(), published.PublishedAtUtc);
            Assert.Null(published.LeaseOwner);
            Assert.Null(published.LeaseExpiresAtUtc);
        }

        [Fact]
        public void TryTransitionTo_Failed_ShouldTruncateAnOverlongFailureReasonAndSetNextRetry()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var publishing = CreateInState(OutboxMessageStatus.Publishing, leaseOwner: "worker-1");
            var overlongReason = new string('x', OutboxMessageLimits.MaxFailureReasonLength + 100);
            var nextRetryAtUtc = timeProvider.GetUtcNow().AddMinutes(1);

            var failed = publishing.TryTransitionTo(
                OutboxMessageStatus.Failed,
                timeProvider,
                nextRetryAtUtc: nextRetryAtUtc,
                failureReason: overlongReason);

            Assert.Equal(OutboxMessageStatus.Failed, failed.Status);
            Assert.Equal(OutboxMessageLimits.MaxFailureReasonLength, failed.FailureReason!.Length);
            Assert.Equal(nextRetryAtUtc, failed.NextRetryAtUtc);
            Assert.Null(failed.LeaseOwner);
        }

        [Fact]
        public void TryTransitionTo_DeadLettered_ShouldStampDeadLetteredAtUtcAndClearLease()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var publishing = CreateInState(OutboxMessageStatus.Publishing, leaseOwner: "worker-1");

            timeProvider.Advance(TimeSpan.FromMinutes(1));
            var deadLettered = publishing.TryTransitionTo(OutboxMessageStatus.DeadLettered, timeProvider, failureReason: "unrecoverable");

            Assert.Equal(OutboxMessageStatus.DeadLettered, deadLettered.Status);
            Assert.Equal(timeProvider.GetUtcNow(), deadLettered.DeadLetteredAtUtc);
            Assert.Null(deadLettered.LeaseOwner);
        }

        [Theory]
        [InlineData(OutboxMessageStatus.Published)]
        [InlineData(OutboxMessageStatus.DeadLettered)]
        public void Requeue_ShouldResetTerminalEntriesToPendingWithANewOrderingSequence(OutboxMessageStatus terminalStatus)
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var terminal = CreateInState(terminalStatus, leaseOwner: null) with
            {
                Attempts = 3,
                FailureReason = "boom",
                OrderingSequence = 1,
            };

            timeProvider.Advance(TimeSpan.FromDays(1));
            var requeued = terminal.Requeue(newOrderingSequence: 42, timeProvider);

            Assert.Equal(OutboxMessageStatus.Pending, requeued.Status);
            Assert.Equal(42, requeued.OrderingSequence);
            Assert.Equal(0, requeued.Attempts);
            Assert.Null(requeued.FailureReason);
            Assert.Null(requeued.LeaseOwner);
            Assert.Null(requeued.PublishedAtUtc);
            Assert.Null(requeued.DeadLetteredAtUtc);
            Assert.Equal(timeProvider.GetUtcNow(), requeued.CreatedAtUtc);
        }

        [Theory]
        [InlineData(OutboxMessageStatus.Pending)]
        [InlineData(OutboxMessageStatus.Publishing)]
        [InlineData(OutboxMessageStatus.Failed)]
        public void Requeue_ShouldRejectNonTerminalEntries(OutboxMessageStatus nonTerminalStatus)
        {
            var message = CreateInState(nonTerminalStatus);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);

            var exception = Assert.Throws<OutboxMessageTransitionException>(() => message.Requeue(newOrderingSequence: 1, timeProvider));

            Assert.Equal(nonTerminalStatus, exception.From);
            Assert.Equal(OutboxMessageStatus.Pending, exception.To);
        }

        [Fact]
        public void Requeue_ShouldNeverProduceAPublishingStatusDirectly()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var requeued = CreateInState(OutboxMessageStatus.Published).Requeue(newOrderingSequence: 1, timeProvider);

            Assert.NotEqual(OutboxMessageStatus.Publishing, requeued.Status);
            Assert.False(requeued.CanTransitionTo(OutboxMessageStatus.Published));
            Assert.True(requeued.CanTransitionTo(OutboxMessageStatus.Publishing));
        }

        [Fact]
        public void CreatePending_ShouldDefaultToEmptyHeadersWhenNoneAreSupplied()
        {
            var message = OutboxMessage.CreatePending(
                Guid.NewGuid(), "message-1", "provider-a", orderingSequence: 1,
                schemaVersion: 1, payloadContentType: "application/json", payload: [],
                headers: null, partitionKey: null, timeProvider: new ManualTimeProvider(DateTimeOffset.UnixEpoch));

            Assert.Empty(message.Headers);
        }

        [Fact]
        public void Headers_ShouldSerializeInTheSameOrderRegardlessOfInsertionOrder()
        {
            var first = OutboxMessage.CreatePending(
                Guid.NewGuid(), "message-1", "provider-a", orderingSequence: 1,
                schemaVersion: 1, payloadContentType: "application/json", payload: [],
                headers: new Dictionary<string, string> { ["zeta"] = "1", ["alpha"] = "2", ["mid"] = "3" },
                partitionKey: null, timeProvider: new ManualTimeProvider(DateTimeOffset.UnixEpoch));

            var second = OutboxMessage.CreatePending(
                Guid.NewGuid(), "message-1", "provider-a", orderingSequence: 1,
                schemaVersion: 1, payloadContentType: "application/json", payload: [],
                headers: new Dictionary<string, string> { ["mid"] = "3", ["zeta"] = "1", ["alpha"] = "2" },
                partitionKey: null, timeProvider: new ManualTimeProvider(DateTimeOffset.UnixEpoch));

            Assert.Equal(["alpha", "mid", "zeta"], first.Headers.Keys);
            Assert.Equal(JsonSerializer.Serialize(first.Headers), JsonSerializer.Serialize(second.Headers));
        }

        [Fact]
        public void JsonRoundTrip_ShouldPreserveEveryField()
        {
            var original = CreateInState(OutboxMessageStatus.Failed, leaseOwner: null) with
            {
                PartitionKey = "partition-a",
                Headers = new Dictionary<string, string> { ["a"] = "1" },
                NextRetryAtUtc = DateTimeOffset.UnixEpoch.AddMinutes(5),
                FailureReason = "boom",
            };

            var json = JsonSerializer.Serialize(original);
            var roundTripped = JsonSerializer.Deserialize<OutboxMessage>(json);

            Assert.NotNull(roundTripped);
            Assert.Equal(original.Id, roundTripped!.Id);
            Assert.Equal(original.MessageId, roundTripped.MessageId);
            Assert.Equal(original.ProviderKey, roundTripped.ProviderKey);
            Assert.Equal(original.PartitionKey, roundTripped.PartitionKey);
            Assert.Equal(original.OrderingSequence, roundTripped.OrderingSequence);
            Assert.Equal(original.SchemaVersion, roundTripped.SchemaVersion);
            Assert.Equal(original.PayloadContentType, roundTripped.PayloadContentType);
            Assert.Equal(original.Payload, roundTripped.Payload);
            Assert.Equal(original.Headers, roundTripped.Headers);
            Assert.Equal(original.Status, roundTripped.Status);
            Assert.Equal(original.Attempts, roundTripped.Attempts);
            Assert.Equal(original.CreatedAtUtc, roundTripped.CreatedAtUtc);
            Assert.Equal(original.NextRetryAtUtc, roundTripped.NextRetryAtUtc);
            Assert.Equal(original.FailureReason, roundTripped.FailureReason);

            // Deterministic: serializing the round-tripped value again produces byte-identical JSON.
            Assert.Equal(json, JsonSerializer.Serialize(roundTripped));
        }

        [Fact]
        public void RenewLease_ShouldExtendExpiryForTheCurrentLeaseOwner()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var publishing = CreateInState(OutboxMessageStatus.Publishing, leaseOwner: "worker-1") with { LeaseExpiresAtUtc = timeProvider.GetUtcNow().AddMinutes(1) };

            timeProvider.Advance(TimeSpan.FromSeconds(50));
            var renewed = publishing.RenewLease(timeProvider, "worker-1", TimeSpan.FromMinutes(1));

            Assert.NotNull(renewed);
            Assert.Equal(OutboxMessageStatus.Publishing, renewed!.Status);
            Assert.Equal("worker-1", renewed.LeaseOwner);
            Assert.Equal(timeProvider.GetUtcNow().AddMinutes(1), renewed.LeaseExpiresAtUtc);
        }

        [Fact]
        public void RenewLease_ShouldRejectAMismatchedLeaseOwner()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var publishing = CreateInState(OutboxMessageStatus.Publishing, leaseOwner: "worker-1");

            Assert.Null(publishing.RenewLease(timeProvider, "worker-2", TimeSpan.FromMinutes(1)));
        }

        [Theory]
        [InlineData(OutboxMessageStatus.Pending)]
        [InlineData(OutboxMessageStatus.Published)]
        [InlineData(OutboxMessageStatus.Failed)]
        [InlineData(OutboxMessageStatus.DeadLettered)]
        public void RenewLease_ShouldRejectNonPublishingEntries(OutboxMessageStatus status)
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var message = CreateInState(status, leaseOwner: null);

            Assert.Null(message.RenewLease(timeProvider, "worker-1", TimeSpan.FromMinutes(1)));
        }

        private static OutboxMessage CreateInState(OutboxMessageStatus status, string? leaseOwner = "worker-1")
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var pending = OutboxMessage.CreatePending(
                Guid.NewGuid(), "message-1", "provider-a", orderingSequence: 1,
                schemaVersion: 1, payloadContentType: "application/json", payload: [1, 2, 3],
                headers: null, partitionKey: null, timeProvider: timeProvider);

            return status switch
            {
                OutboxMessageStatus.Pending => pending,
                OutboxMessageStatus.Publishing => pending.TryTransitionTo(OutboxMessageStatus.Publishing, timeProvider, leaseOwner: leaseOwner),
                OutboxMessageStatus.Published => pending
                    .TryTransitionTo(OutboxMessageStatus.Publishing, timeProvider, leaseOwner: leaseOwner)
                    .TryTransitionTo(OutboxMessageStatus.Published, timeProvider),
                OutboxMessageStatus.Failed => pending
                    .TryTransitionTo(OutboxMessageStatus.Publishing, timeProvider, leaseOwner: leaseOwner)
                    .TryTransitionTo(OutboxMessageStatus.Failed, timeProvider),
                OutboxMessageStatus.DeadLettered => pending
                    .TryTransitionTo(OutboxMessageStatus.Publishing, timeProvider, leaseOwner: leaseOwner)
                    .TryTransitionTo(OutboxMessageStatus.DeadLettered, timeProvider),
                _ => throw new ArgumentOutOfRangeException(nameof(status), status, null),
            };
        }
    }
}
