using Microsoft.Extensions.Diagnostics.HealthChecks;
using NSubstitute;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    // ChannelKey is per-instance (xUnit creates a fresh instance per test method), never static -
    // InboxHealthCheck taps InboxTelemetry.DeadLettered process-wide, so a shared literal would let
    // concurrently-running tests' dead-letter counts bleed into each other.
    public class InboxHealthCheckTests
    {
        private readonly Guid ChannelKey = Guid.NewGuid();
        private readonly Guid FeederId = Guid.NewGuid();

        [Fact]
        public async Task CheckHealthAsync_StoreUnhealthy_ShouldReturnUnhealthyImmediatelyWithoutProbingTheBacklog()
        {
            var store = Substitute.For<IInboxStore, IHealthCheck>();
            ((IHealthCheck)store).CheckHealthAsync(Arg.Any<HealthCheckContext>(), Arg.Any<CancellationToken>())
                .Returns(HealthCheckResult.Unhealthy("Redis connection is unavailable."));
            using var check = new InboxHealthCheck(ChannelKey, store);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            await store.DidNotReceiveWithAnyArgs().QueryRetryableAsync(default, default, default);
        }

        [Fact]
        public async Task CheckHealthAsync_NoBacklogNoHeartbeatConfigured_ShouldReturnHealthy()
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            using var check = new InboxHealthCheck(ChannelKey, store, new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null });

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Healthy, result.Status);
        }

        [Theory]
        [InlineData(50, HealthStatus.Degraded)]
        [InlineData(100, HealthStatus.Unhealthy)]
        [InlineData(49, HealthStatus.Healthy)]
        public async Task CheckHealthAsync_BacklogThresholds_ShouldReportTheDocumentedStatus(int backlogCount, HealthStatus expected)
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(Enumerable.Range(0, backlogCount).Select(i => BuildMessage($"m{i}", DateTimeOffset.UtcNow)).ToList());
            var options = new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedBacklogCount = 50, UnhealthyBacklogCount = 100 };
            using var check = new InboxHealthCheck(ChannelKey, store, options);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(expected, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_OldestEntryAtOrBeyondUnhealthyAge_ShouldReturnUnhealthy()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var oldEntry = BuildMessage("old", timeProvider.GetUtcNow());
            timeProvider.Advance(TimeSpan.FromHours(2));

            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([oldEntry]);
            var options = new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedBacklogCount = 1000, UnhealthyBacklogCount = 2000, UnhealthyOldestAge = TimeSpan.FromHours(1) };
            using var check = new InboxHealthCheck(ChannelKey, store, options, timeProvider: timeProvider);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Equal(TimeSpan.FromHours(2).TotalSeconds, result.Data["oldest_age_seconds"]);
        }

        [Fact]
        public async Task CheckHealthAsync_DeadLetterCountAtOrAboveUnhealthyThreshold_ShouldReturnUnhealthy()
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            var options = new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedDeadLetterCount = 5, UnhealthyDeadLetterCount = 10 };
            using var check = new InboxHealthCheck(ChannelKey, store, options);

            InboxTelemetry.DeadLettered.Add(10, new KeyValuePair<string, object?>(InboxTelemetry.TagChannel, ChannelKey));

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Equal(10L, result.Data["dead_lettered_since_start"]);
        }

        [Fact]
        public async Task CheckHealthAsync_DeadLetterCountForADifferentChannel_ShouldNotCount()
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            var options = new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedDeadLetterCount = 1 };
            using var check = new InboxHealthCheck(ChannelKey, store, options);

            InboxTelemetry.DeadLettered.Add(10, new KeyValuePair<string, object?>(InboxTelemetry.TagChannel, Guid.NewGuid()));

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Equal(0L, result.Data["dead_lettered_since_start"]);
        }

        [Fact]
        public async Task CheckHealthAsync_NoWorkerHasPolledYet_ShouldReturnDegraded()
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            var heartbeat = new FixedHeartbeat(new Dictionary<Guid, DateTimeOffset>());
            using var check = new InboxHealthCheck(ChannelKey, store, workerHeartbeat: heartbeat);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Degraded, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_WorkerHeartbeatStale_ShouldReturnUnhealthy()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var heartbeat = new FixedHeartbeat(new Dictionary<Guid, DateTimeOffset> { [ChannelKey] = timeProvider.GetUtcNow() });
            timeProvider.Advance(TimeSpan.FromMinutes(10));

            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            var options = new InboxHealthCheckOptions { WorkerHeartbeatTimeout = TimeSpan.FromMinutes(5) };
            using var check = new InboxHealthCheck(ChannelKey, store, options, heartbeat, timeProvider);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_WorkerHeartbeatRecent_ShouldStayHealthy()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var heartbeat = new FixedHeartbeat(new Dictionary<Guid, DateTimeOffset> { [ChannelKey] = timeProvider.GetUtcNow() });
            timeProvider.Advance(TimeSpan.FromSeconds(30));

            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            var options = new InboxHealthCheckOptions { WorkerHeartbeatTimeout = TimeSpan.FromMinutes(5) };
            using var check = new InboxHealthCheck(ChannelKey, store, options, heartbeat, timeProvider);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Healthy, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_ShouldProbeWithTheConfiguredBoundedBatchSizeNeverUnbounded()
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            var options = new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null, ProbeBatchSize = 37 };
            using var check = new InboxHealthCheck(ChannelKey, store, options);

            await check.CheckHealthAsync(new HealthCheckContext());

            await store.Received(1).QueryRetryableAsync(ChannelKey, 37, Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task CheckHealthAsync_CancellationFromTheStore_ShouldPropagateNotBeReportedAsUnhealthy()
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromException<IReadOnlyList<InboxMessage>>(new OperationCanceledException()));
            using var check = new InboxHealthCheck(ChannelKey, store, new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null });

            await Assert.ThrowsAsync<OperationCanceledException>(() => check.CheckHealthAsync(new HealthCheckContext()));
        }

        [Fact]
        public async Task CheckHealthAsync_Data_ShouldNeverContainAConnectionStringPayloadOrMessageId()
        {
            var store = Substitute.For<IInboxStore>();
            store.QueryRetryableAsync(ChannelKey, Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns([BuildMessage("secret-message-id-42", DateTimeOffset.UtcNow)]);
            using var check = new InboxHealthCheck(ChannelKey, store, new InboxHealthCheckOptions { WorkerHeartbeatTimeout = null });

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            var allowedKeys = new HashSet<string> { "channel", "store_status", "backlog_count", "backlog_count_is_lower_bound", "oldest_age_seconds", "dead_lettered_since_start", "last_poll_utc" };
            Assert.All(result.Data.Keys, key => Assert.Contains(key, allowedKeys));
            Assert.DoesNotContain(result.Data.Values, v => v is string s && s.Contains("secret-message-id-42"));
        }

        private InboxMessage BuildMessage(string messageId, DateTimeOffset receivedAtUtc) =>
            InboxMessage.CreateReceived(
                Guid.NewGuid(), messageId, ChannelKey, FeederId,
                schemaVersion: 1, payloadContentType: "application/octet-stream", payload: [1, 2, 3],
                headers: null, partitionKey: null, timeProvider: new ManualTimeProvider(receivedAtUtc));

        private sealed class FixedHeartbeat(IReadOnlyDictionary<Guid, DateTimeOffset> lastPolledAtUtc) : IInboxWorkerHeartbeat
        {
            public IReadOnlyDictionary<Guid, DateTimeOffset> LastPolledAtUtc => lastPolledAtUtc;
        }
    }
}
