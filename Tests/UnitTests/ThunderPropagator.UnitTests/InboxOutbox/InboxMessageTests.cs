using System.Text.Json;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxMessageTests
    {
        private static readonly InboxMessageStatus[] AllStatuses =
        [
            InboxMessageStatus.Received,
            InboxMessageStatus.Processing,
            InboxMessageStatus.Processed,
            InboxMessageStatus.Failed,
            InboxMessageStatus.DeadLettered,
        ];

        private static readonly HashSet<(InboxMessageStatus From, InboxMessageStatus To)> AllowedTransitions =
        [
            (InboxMessageStatus.Received, InboxMessageStatus.Processing),
            (InboxMessageStatus.Processing, InboxMessageStatus.Processed),
            (InboxMessageStatus.Processing, InboxMessageStatus.Failed),
            (InboxMessageStatus.Processing, InboxMessageStatus.DeadLettered),
            (InboxMessageStatus.Failed, InboxMessageStatus.Processing),
            (InboxMessageStatus.Failed, InboxMessageStatus.DeadLettered),
        ];

        public static TheoryData<InboxMessageStatus, InboxMessageStatus, bool> AllTransitionPairs()
        {
            var data = new TheoryData<InboxMessageStatus, InboxMessageStatus, bool>();

            foreach (var from in AllStatuses)
            foreach (var to in AllStatuses)
                data.Add(from, to, AllowedTransitions.Contains((from, to)));

            return data;
        }

        [Theory]
        [MemberData(nameof(AllTransitionPairs))]
        public void CanTransitionTo_ShouldMatchTheAllowedTransitionTable(InboxMessageStatus from, InboxMessageStatus to, bool expectedAllowed)
        {
            var message = CreateInState(from);

            Assert.Equal(expectedAllowed, message.CanTransitionTo(to));
        }

        [Theory]
        [MemberData(nameof(AllTransitionPairs))]
        public void TryTransitionTo_ShouldThrowOnlyForDisallowedTransitions(InboxMessageStatus from, InboxMessageStatus to, bool expectedAllowed)
        {
            var message = CreateInState(from);
            var timeProvider = new TestTimeProvider(DateTimeOffset.UnixEpoch);

            if (expectedAllowed)
            {
                var result = message.TryTransitionTo(to, timeProvider);
                Assert.Equal(to, result.Status);
            }
            else
            {
                var exception = Assert.Throws<InboxMessageTransitionException>(() => message.TryTransitionTo(to, timeProvider));
                Assert.Equal(from, exception.From);
                Assert.Equal(to, exception.To);
            }
        }

        [Fact]
        public void TryTransitionTo_Processing_ShouldStampLeaseAndIncrementAttemptCountWhenRequested()
        {
            var timeProvider = new TestTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
            var received = CreateInState(InboxMessageStatus.Received);
            var leaseExpiresAtUtc = timeProvider.GetUtcNow().AddMinutes(5);

            var processing = received.TryTransitionTo(
                InboxMessageStatus.Processing,
                timeProvider,
                leaseOwner: "worker-1",
                leaseExpiresAtUtc: leaseExpiresAtUtc,
                incrementAttempt: true);

            Assert.Equal(InboxMessageStatus.Processing, processing.Status);
            Assert.Equal(1, processing.AttemptCount);
            Assert.Equal("worker-1", processing.LeaseOwner);
            Assert.Equal(leaseExpiresAtUtc, processing.LeaseExpiresAtUtc);
            Assert.Equal(timeProvider.GetUtcNow(), processing.ProcessingStartedAtUtc);
        }

        [Fact]
        public void TryTransitionTo_Processed_ShouldClearLeaseAndStampProcessedAtUtc()
        {
            var timeProvider = new TestTimeProvider(DateTimeOffset.UnixEpoch);
            var processing = CreateInState(InboxMessageStatus.Processing, leaseOwner: "worker-1");

            timeProvider.Advance(TimeSpan.FromSeconds(30));
            var processed = processing.TryTransitionTo(InboxMessageStatus.Processed, timeProvider);

            Assert.Equal(InboxMessageStatus.Processed, processed.Status);
            Assert.Equal(timeProvider.GetUtcNow(), processed.ProcessedAtUtc);
            Assert.Null(processed.LeaseOwner);
            Assert.Null(processed.LeaseExpiresAtUtc);
        }

        [Fact]
        public void TryTransitionTo_Failed_ShouldTruncateAnOverlongFailureReasonAndSetNextRetry()
        {
            var timeProvider = new TestTimeProvider(DateTimeOffset.UnixEpoch);
            var processing = CreateInState(InboxMessageStatus.Processing, leaseOwner: "worker-1");
            var overlongReason = new string('x', InboxMessageLimits.MaxFailureReasonLength + 100);
            var nextRetryAtUtc = timeProvider.GetUtcNow().AddMinutes(1);

            var failed = processing.TryTransitionTo(
                InboxMessageStatus.Failed,
                timeProvider,
                nextRetryAtUtc: nextRetryAtUtc,
                failureReason: overlongReason);

            Assert.Equal(InboxMessageStatus.Failed, failed.Status);
            Assert.Equal(InboxMessageLimits.MaxFailureReasonLength, failed.FailureReason!.Length);
            Assert.Equal(nextRetryAtUtc, failed.NextRetryAtUtc);
            Assert.Null(failed.LeaseOwner);
        }

        [Theory]
        [InlineData(InboxMessageStatus.Processed)]
        [InlineData(InboxMessageStatus.DeadLettered)]
        public void Replay_ShouldResetTerminalEntriesToReceived(InboxMessageStatus terminalStatus)
        {
            var timeProvider = new TestTimeProvider(DateTimeOffset.UnixEpoch);
            var terminal = CreateInState(terminalStatus, leaseOwner: null) with
            {
                AttemptCount = 3,
                FailureReason = "boom",
            };

            timeProvider.Advance(TimeSpan.FromDays(1));
            var replayed = terminal.Replay(timeProvider);

            Assert.Equal(InboxMessageStatus.Received, replayed.Status);
            Assert.Equal(0, replayed.AttemptCount);
            Assert.Null(replayed.FailureReason);
            Assert.Null(replayed.LeaseOwner);
            Assert.Equal(timeProvider.GetUtcNow(), replayed.ReceivedAtUtc);
        }

        [Theory]
        [InlineData(InboxMessageStatus.Received)]
        [InlineData(InboxMessageStatus.Processing)]
        [InlineData(InboxMessageStatus.Failed)]
        public void Replay_ShouldRejectNonTerminalEntries(InboxMessageStatus nonTerminalStatus)
        {
            var message = CreateInState(nonTerminalStatus);
            var timeProvider = new TestTimeProvider(DateTimeOffset.UnixEpoch);

            var exception = Assert.Throws<InboxMessageTransitionException>(() => message.Replay(timeProvider));

            Assert.Equal(nonTerminalStatus, exception.From);
            Assert.Equal(InboxMessageStatus.Received, exception.To);
        }

        [Fact]
        public void Replay_ShouldNeverProduceAProcessingStatusDirectly()
        {
            var timeProvider = new TestTimeProvider(DateTimeOffset.UnixEpoch);
            var replayed = CreateInState(InboxMessageStatus.Processed).Replay(timeProvider);

            Assert.NotEqual(InboxMessageStatus.Processing, replayed.Status);
            Assert.False(replayed.CanTransitionTo(InboxMessageStatus.Processed));
            Assert.True(replayed.CanTransitionTo(InboxMessageStatus.Processing));
        }

        [Fact]
        public void CreateReceived_ShouldDefaultToEmptyHeadersWhenNoneAreSupplied()
        {
            var message = InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-1", Guid.NewGuid(), Guid.NewGuid(),
                schemaVersion: 1, payloadContentType: "application/json", payload: [],
                headers: null, partitionKey: null, timeProvider: new TestTimeProvider(DateTimeOffset.UnixEpoch));

            Assert.Empty(message.Headers);
        }

        [Fact]
        public void Headers_ShouldSerializeInTheSameOrderRegardlessOfInsertionOrder()
        {
            var first = InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-1", Guid.NewGuid(), Guid.NewGuid(),
                schemaVersion: 1, payloadContentType: "application/json", payload: [],
                headers: new Dictionary<string, string> { ["zeta"] = "1", ["alpha"] = "2", ["mid"] = "3" },
                partitionKey: null, timeProvider: new TestTimeProvider(DateTimeOffset.UnixEpoch));

            var second = InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-1", Guid.NewGuid(), Guid.NewGuid(),
                schemaVersion: 1, payloadContentType: "application/json", payload: [],
                headers: new Dictionary<string, string> { ["mid"] = "3", ["zeta"] = "1", ["alpha"] = "2" },
                partitionKey: null, timeProvider: new TestTimeProvider(DateTimeOffset.UnixEpoch));

            Assert.Equal(["alpha", "mid", "zeta"], first.Headers.Keys);
            Assert.Equal(JsonSerializer.Serialize(first.Headers), JsonSerializer.Serialize(second.Headers));
        }

        private static InboxMessage CreateInState(InboxMessageStatus status, string? leaseOwner = "worker-1")
        {
            var timeProvider = new TestTimeProvider(DateTimeOffset.UnixEpoch);
            var received = InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-1", Guid.NewGuid(), Guid.NewGuid(),
                schemaVersion: 1, payloadContentType: "application/json", payload: [1, 2, 3],
                headers: null, partitionKey: null, timeProvider: timeProvider);

            return status switch
            {
                InboxMessageStatus.Received => received,
                InboxMessageStatus.Processing => received.TryTransitionTo(InboxMessageStatus.Processing, timeProvider, leaseOwner: leaseOwner),
                InboxMessageStatus.Processed => received
                    .TryTransitionTo(InboxMessageStatus.Processing, timeProvider, leaseOwner: leaseOwner)
                    .TryTransitionTo(InboxMessageStatus.Processed, timeProvider),
                InboxMessageStatus.Failed => received
                    .TryTransitionTo(InboxMessageStatus.Processing, timeProvider, leaseOwner: leaseOwner)
                    .TryTransitionTo(InboxMessageStatus.Failed, timeProvider),
                InboxMessageStatus.DeadLettered => received
                    .TryTransitionTo(InboxMessageStatus.Processing, timeProvider, leaseOwner: leaseOwner)
                    .TryTransitionTo(InboxMessageStatus.DeadLettered, timeProvider),
                _ => throw new ArgumentOutOfRangeException(nameof(status), status, null),
            };
        }

        private sealed class TestTimeProvider(DateTimeOffset now) : TimeProvider
        {
            private DateTimeOffset _now = now;

            public override DateTimeOffset GetUtcNow() => _now;

            public void Advance(TimeSpan by) => _now += by;
        }
    }
}
