namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Per-Provider degraded/unhealthy thresholds for <see cref="OutboxHealthCheck"/>.</summary>
    public sealed record OutboxHealthCheckOptions
    {
        /// <summary>Pending+backlog depth (see <see cref="IOutboxStore.GetDepthAsync"/>) at or above which the Provider reports Degraded.</summary>
        public int DegradedBacklogCount { get; init; } = 50;

        /// <summary>Backlog depth at or above which the Provider reports Unhealthy. Must be at least <see cref="DegradedBacklogCount"/>.</summary>
        public int UnhealthyBacklogCount { get; init; } = 100;

        /// <summary>Age of the oldest Pending entry (see <see cref="IOutboxStore.GetOldestPendingAgeAsync"/>) at or beyond which the Provider reports Degraded.</summary>
        public TimeSpan DegradedOldestAge { get; init; } = TimeSpan.FromMinutes(15);

        /// <summary>Age of the oldest Pending entry at or beyond which the Provider reports Unhealthy. Must be at least <see cref="DegradedOldestAge"/>.</summary>
        public TimeSpan UnhealthyOldestAge { get; init; } = TimeSpan.FromHours(1);

        /// <summary>Dead-lettered entries observed since this check was constructed at or above which the Provider reports Degraded.</summary>
        public int DegradedDeadLetterCount { get; init; } = 10;

        /// <summary>Dead-lettered entries observed since this check was constructed at or above which the Provider reports Unhealthy. Must be at least <see cref="DegradedDeadLetterCount"/>.</summary>
        public int UnhealthyDeadLetterCount { get; init; } = 50;

        /// <summary>
        /// How long a Provider may go without a worker poll (see <see cref="IOutboxWorkerHeartbeat"/>)
        /// before it reports Unhealthy - a stuck or never-started worker means entries stop moving even
        /// though the store itself is perfectly healthy. <see langword="null"/> disables the heartbeat
        /// check entirely (e.g. no <c>OutboxRelayWorker</c> is wired up for this Provider by design).
        /// </summary>
        public TimeSpan? WorkerHeartbeatTimeout { get; init; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Validates every cross-field rule below, throwing <see cref="ArgumentException"/> on the first
        /// violation.
        /// </summary>
        public void Validate()
        {
            if (UnhealthyBacklogCount < DegradedBacklogCount)
                throw new ArgumentException($"{nameof(UnhealthyBacklogCount)} must be at least {nameof(DegradedBacklogCount)}.", nameof(UnhealthyBacklogCount));

            if (UnhealthyOldestAge < DegradedOldestAge)
                throw new ArgumentException($"{nameof(UnhealthyOldestAge)} must be at least {nameof(DegradedOldestAge)}.", nameof(UnhealthyOldestAge));

            if (UnhealthyDeadLetterCount < DegradedDeadLetterCount)
                throw new ArgumentException($"{nameof(UnhealthyDeadLetterCount)} must be at least {nameof(DegradedDeadLetterCount)}.", nameof(UnhealthyDeadLetterCount));

            if (WorkerHeartbeatTimeout is { } timeout && timeout <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(WorkerHeartbeatTimeout)} must be positive when set.", nameof(WorkerHeartbeatTimeout));
        }
    }
}
