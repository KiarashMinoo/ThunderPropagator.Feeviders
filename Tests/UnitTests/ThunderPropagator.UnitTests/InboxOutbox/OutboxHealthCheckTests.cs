using Microsoft.Extensions.Diagnostics.HealthChecks;
using NSubstitute;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    // ProviderKey is per-instance (xUnit creates a fresh instance per test method), never a shared
    // literal - OutboxHealthCheck taps OutboxTelemetry.DeadLettered process-wide, so a shared literal
    // would let concurrently-running tests' dead-letter counts bleed into each other.
    public class OutboxHealthCheckTests
    {
        private readonly string ProviderKey = $"provider-{Guid.NewGuid():N}";

        [Fact]
        public async Task CheckHealthAsync_StoreUnhealthy_ShouldReturnUnhealthyImmediatelyWithoutProbingDepth()
        {
            var store = Substitute.For<IOutboxStore, IHealthCheck>();
            ((IHealthCheck)store).CheckHealthAsync(Arg.Any<HealthCheckContext>(), Arg.Any<CancellationToken>())
                .Returns(HealthCheckResult.Unhealthy("Redis connection is unavailable."));
            using var check = new OutboxHealthCheck(ProviderKey, store);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            await store.DidNotReceiveWithAnyArgs().GetDepthAsync(default, default);
        }

        [Fact]
        public async Task CheckHealthAsync_NoBacklogNoHeartbeatConfigured_ShouldReturnHealthy()
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(0);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns((TimeSpan?)null);
            using var check = new OutboxHealthCheck(ProviderKey, store, new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = null });

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Healthy, result.Status);
        }

        [Theory]
        [InlineData(50, HealthStatus.Degraded)]
        [InlineData(100, HealthStatus.Unhealthy)]
        [InlineData(49, HealthStatus.Healthy)]
        public async Task CheckHealthAsync_BacklogThresholds_ShouldReportTheDocumentedStatus(int depth, HealthStatus expected)
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(depth);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns((TimeSpan?)null);
            var options = new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedBacklogCount = 50, UnhealthyBacklogCount = 100 };
            using var check = new OutboxHealthCheck(ProviderKey, store, options);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(expected, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_OldestPendingAgeAtOrBeyondUnhealthy_ShouldReturnUnhealthy()
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(1);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns(TimeSpan.FromHours(2));
            var options = new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedBacklogCount = 1000, UnhealthyBacklogCount = 2000, UnhealthyOldestAge = TimeSpan.FromHours(1) };
            using var check = new OutboxHealthCheck(ProviderKey, store, options);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Equal(TimeSpan.FromHours(2).TotalSeconds, result.Data["oldest_age_seconds"]);
        }

        [Fact]
        public async Task CheckHealthAsync_DeadLetterCountAtOrAboveUnhealthyThreshold_ShouldReturnUnhealthy()
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(0);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns((TimeSpan?)null);
            var options = new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedDeadLetterCount = 5, UnhealthyDeadLetterCount = 10 };
            using var check = new OutboxHealthCheck(ProviderKey, store, options);

            OutboxTelemetry.DeadLettered.Add(10, new KeyValuePair<string, object?>(OutboxTelemetry.TagProvider, ProviderKey));

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Equal(10L, result.Data["dead_lettered_since_start"]);
        }

        [Fact]
        public async Task CheckHealthAsync_DeadLetterCountForADifferentProvider_ShouldNotCount()
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(0);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns((TimeSpan?)null);
            var options = new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = null, DegradedDeadLetterCount = 1 };
            using var check = new OutboxHealthCheck(ProviderKey, store, options);

            OutboxTelemetry.DeadLettered.Add(10, new KeyValuePair<string, object?>(OutboxTelemetry.TagProvider, $"provider-{Guid.NewGuid():N}"));

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Equal(0L, result.Data["dead_lettered_since_start"]);
        }

        [Fact]
        public async Task CheckHealthAsync_NoWorkerHasPolledYet_ShouldReturnDegraded()
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(0);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns((TimeSpan?)null);
            var heartbeat = new FixedHeartbeat(new Dictionary<string, DateTimeOffset>());
            using var check = new OutboxHealthCheck(ProviderKey, store, workerHeartbeat: heartbeat);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Degraded, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_WorkerHeartbeatStale_ShouldReturnUnhealthy()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var heartbeat = new FixedHeartbeat(new Dictionary<string, DateTimeOffset> { [ProviderKey] = timeProvider.GetUtcNow() });
            timeProvider.Advance(TimeSpan.FromMinutes(10));

            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(0);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns((TimeSpan?)null);
            var options = new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = TimeSpan.FromMinutes(5) };
            using var check = new OutboxHealthCheck(ProviderKey, store, options, heartbeat, timeProvider);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_WorkerHeartbeatRecent_ShouldStayHealthy()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var heartbeat = new FixedHeartbeat(new Dictionary<string, DateTimeOffset> { [ProviderKey] = timeProvider.GetUtcNow() });
            timeProvider.Advance(TimeSpan.FromSeconds(30));

            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(0);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns((TimeSpan?)null);
            var options = new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = TimeSpan.FromMinutes(5) };
            using var check = new OutboxHealthCheck(ProviderKey, store, options, heartbeat, timeProvider);

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Healthy, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_CancellationFromTheStore_ShouldPropagateNotBeReportedAsUnhealthy()
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(Task.FromException<int>(new OperationCanceledException()));
            using var check = new OutboxHealthCheck(ProviderKey, store, new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = null });

            await Assert.ThrowsAsync<OperationCanceledException>(() => check.CheckHealthAsync(new HealthCheckContext()));
        }

        [Fact]
        public async Task CheckHealthAsync_Data_ShouldNeverContainAConnectionStringPayloadOrMessageId()
        {
            var store = Substitute.For<IOutboxStore>();
            store.GetDepthAsync(null, Arg.Any<CancellationToken>()).Returns(1);
            store.GetOldestPendingAgeAsync(null, Arg.Any<TimeProvider>(), Arg.Any<CancellationToken>()).Returns(TimeSpan.FromMinutes(1));
            using var check = new OutboxHealthCheck(ProviderKey, store, new OutboxHealthCheckOptions { WorkerHeartbeatTimeout = null });

            var result = await check.CheckHealthAsync(new HealthCheckContext());

            var allowedKeys = new HashSet<string> { "provider", "store_status", "backlog_count", "oldest_age_seconds", "dead_lettered_since_start", "last_poll_utc" };
            Assert.All(result.Data.Keys, key => Assert.Contains(key, allowedKeys));
        }

        private sealed class FixedHeartbeat(IReadOnlyDictionary<string, DateTimeOffset> lastPolledAtUtc) : IOutboxWorkerHeartbeat
        {
            public IReadOnlyDictionary<string, DateTimeOffset> LastPolledAtUtc => lastPolledAtUtc;
        }
    }
}
