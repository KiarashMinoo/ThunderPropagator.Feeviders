namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Per-channel degraded/unhealthy thresholds for <see cref="InboxHealthCheck"/>.</summary>
    public sealed record InboxHealthCheckOptions
    {
        /// <summary>
        /// How many retry-eligible entries <see cref="InboxHealthCheck"/> asks
        /// <see cref="IInboxStore.QueryRetryableAsync"/> for - a bound, not necessarily the true backlog
        /// size: if exactly this many come back, the real backlog is "at least this many," not "exactly."
        /// Keeps the check itself O(this), never a full scan, regardless of how large the real backlog is.
        /// </summary>
        public int ProbeBatchSize { get; init; } = 100;

        /// <summary>Backlog count (see <see cref="ProbeBatchSize"/>) at or above which the channel reports Degraded.</summary>
        public int DegradedBacklogCount { get; init; } = 50;

        /// <summary>Backlog count at or above which the channel reports Unhealthy. Must be at least <see cref="DegradedBacklogCount"/>.</summary>
        public int UnhealthyBacklogCount { get; init; } = 100;

        /// <summary>Age of the oldest entry in the probed backlog at or beyond which the channel reports Degraded.</summary>
        public TimeSpan DegradedOldestAge { get; init; } = TimeSpan.FromMinutes(15);

        /// <summary>Age of the oldest entry in the probed backlog at or beyond which the channel reports Unhealthy. Must be at least <see cref="DegradedOldestAge"/>.</summary>
        public TimeSpan UnhealthyOldestAge { get; init; } = TimeSpan.FromHours(1);

        /// <summary>Dead-lettered entries observed since this check was constructed at or above which the channel reports Degraded.</summary>
        public int DegradedDeadLetterCount { get; init; } = 10;

        /// <summary>Dead-lettered entries observed since this check was constructed at or above which the channel reports Unhealthy. Must be at least <see cref="DegradedDeadLetterCount"/>.</summary>
        public int UnhealthyDeadLetterCount { get; init; } = 50;

        /// <summary>
        /// How long a channel may go without a worker poll (see <see cref="IInboxWorkerHeartbeat"/>)
        /// before it reports Unhealthy - a stuck or never-started worker means entries stop moving even
        /// though the store itself is perfectly healthy. <see langword="null"/> disables the heartbeat
        /// check entirely (e.g. no <see cref="InboxRetryWorker"/> is wired up for this channel by design).
        /// </summary>
        public TimeSpan? WorkerHeartbeatTimeout { get; init; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Validates every cross-field rule below, throwing <see cref="ArgumentException"/> on the first
        /// violation.
        /// </summary>
        public void Validate()
        {
            if (ProbeBatchSize <= 0)
                throw new ArgumentException($"{nameof(ProbeBatchSize)} must be positive.", nameof(ProbeBatchSize));

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
