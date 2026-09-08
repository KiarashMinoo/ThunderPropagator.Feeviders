using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Reports one channel's Inbox health beyond raw store connectivity: backlog depth, oldest-eligible
    /// age, dead-letter growth, and worker heartbeat - the signals needed to tell "the store is
    /// unavailable," "messages are piling up," and "the retry worker is stuck" apart from each other and
    /// from ordinary, healthy operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Register as a singleton.</b> This type taps <see cref="InboxTelemetry.DeadLettered"/> via a
    /// <see cref="MeterListener"/> to accumulate a dead-letter count since construction (there is no
    /// store-level "count of dead-lettered entries" primitive cheap enough to poll without a full scan -
    /// see the class remarks on <see cref="InboxHealthCheckOptions.ProbeBatchSize"/> for why backlog/age
    /// use a bounded probe instead of an exact count too). Constructing a new instance per check would
    /// reset that accumulator to zero every time, making the dead-letter threshold meaningless - dispose
    /// it exactly once, when the health check itself is disposed (e.g. by the DI container at shutdown).
    /// </para>
    /// <para>
    /// <b>Never a full scan.</b> Backlog/age come from <see cref="IInboxStore.QueryRetryableAsync"/>
    /// bounded to <see cref="InboxHealthCheckOptions.ProbeBatchSize"/> entries - a real backlog far larger
    /// than that bound is reported as "at least <c>ProbeBatchSize</c>," which is all a health check needs
    /// to declare Unhealthy; it never needs (or attempts) the exact count.
    /// </para>
    /// <para>
    /// <b>Never exposes secrets.</b> <see cref="HealthCheckResult.Data"/> carries only counts, ages, a
    /// status string, and the channel key itself - never a connection string (the store's own
    /// <see cref="IHealthCheck"/>, if any, is trusted to keep its own description free of one), a payload,
    /// or a <see cref="InboxMessage.MessageId"/>.
    /// </para>
    /// </remarks>
    public sealed class InboxHealthCheck : IHealthCheck, IDisposable
    {
        private readonly Guid _channelKey;
        private readonly IInboxStore _store;
        private readonly InboxHealthCheckOptions _options;
        private readonly IInboxWorkerHeartbeat? _workerHeartbeat;
        private readonly TimeProvider _timeProvider;
        private readonly MeterListener _deadLetterListener;
        private long _deadLetteredSinceStart;

        /// <param name="channelKey">The channel this instance reports on - matches <see cref="InboxMessage.ChannelKey"/>.</param>
        /// <param name="store">The channel's store. If it also implements <see cref="IHealthCheck"/>, that check's result gates everything else here - see <see cref="CheckHealthAsync"/>.</param>
        /// <param name="options">Thresholds. Defaults applied (and validated) when omitted.</param>
        /// <param name="workerHeartbeat">
        /// The <see cref="InboxRetryWorker"/> polling this channel, if any - omit only when this channel
        /// deliberately has no retry worker wired up, in which case set
        /// <see cref="InboxHealthCheckOptions.WorkerHeartbeatTimeout"/> to <see langword="null"/> too, or
        /// every check reports Degraded for a heartbeat that will never come.
        /// </param>
        /// <param name="timeProvider">Clock used for age/heartbeat comparisons. Defaults to <see cref="TimeProvider.System"/> - tests should supply a fake for deterministic threshold assertions.</param>
        /// <exception cref="ArgumentException"><paramref name="options"/> fails <see cref="InboxHealthCheckOptions.Validate"/>.</exception>
        public InboxHealthCheck(
            Guid channelKey,
            IInboxStore store,
            InboxHealthCheckOptions? options = null,
            IInboxWorkerHeartbeat? workerHeartbeat = null,
            TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(store);

            _channelKey = channelKey;
            _store = store;
            _options = options ?? new InboxHealthCheckOptions();
            _options.Validate();
            _workerHeartbeat = workerHeartbeat;
            _timeProvider = timeProvider ?? TimeProvider.System;

            var channelTagValue = channelKey.ToString();
            _deadLetterListener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument, InboxTelemetry.DeadLettered))
                        listener.EnableMeasurementEvents(instrument);
                },
            };
            _deadLetterListener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == InboxTelemetry.TagChannel && Equals(tag.Value?.ToString(), channelTagValue))
                    {
                        Interlocked.Add(ref _deadLetteredSinceStart, measurement);
                        return;
                    }
                }
            });
            _deadLetterListener.Start();
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var data = new Dictionary<string, object>{ ["channel"] = _channelKey.ToString() };

            if (_store is IHealthCheck storeHealthCheck)
            {
                HealthCheckResult storeResult;
                try
                {
                    storeResult = await storeHealthCheck.CheckHealthAsync(context, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    return HealthCheckResult.Unhealthy("Inbox store threw while checking connectivity.", exception, data);
                }

                if (storeResult.Status != HealthStatus.Healthy)
                {
                    data["store_status"] = storeResult.Status.ToString();
                    return new HealthCheckResult(storeResult.Status, storeResult.Description, storeResult.Exception, data);
                }
            }

            var status = HealthStatus.Healthy;
            var descriptions = new List<string>();

            IReadOnlyList<InboxMessage> candidates;
            try
            {
                candidates = await _store.QueryRetryableAsync(_channelKey, _options.ProbeBatchSize, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                return HealthCheckResult.Unhealthy("Inbox store threw while probing the retry backlog.", exception, data);
            }

            data["backlog_count"] = candidates.Count;
            data["backlog_count_is_lower_bound"] = candidates.Count >= _options.ProbeBatchSize;

            if (candidates.Count >= _options.UnhealthyBacklogCount)
            {
                status = Worse(status, HealthStatus.Unhealthy);
                descriptions.Add($"Backlog is at least {candidates.Count} (unhealthy threshold {_options.UnhealthyBacklogCount}).");
            }
            else if (candidates.Count >= _options.DegradedBacklogCount)
            {
                status = Worse(status, HealthStatus.Degraded);
                descriptions.Add($"Backlog is at least {candidates.Count} (degraded threshold {_options.DegradedBacklogCount}).");
            }

            if (candidates.Count > 0)
            {
                var oldestAge = _timeProvider.GetUtcNow() - candidates.Min(m => m.ReceivedAtUtc);
                data["oldest_age_seconds"] = oldestAge.TotalSeconds;

                if (oldestAge >= _options.UnhealthyOldestAge)
                {
                    status = Worse(status, HealthStatus.Unhealthy);
                    descriptions.Add($"Oldest backlog entry is {oldestAge} old (unhealthy threshold {_options.UnhealthyOldestAge}).");
                }
                else if (oldestAge >= _options.DegradedOldestAge)
                {
                    status = Worse(status, HealthStatus.Degraded);
                    descriptions.Add($"Oldest backlog entry is {oldestAge} old (degraded threshold {_options.DegradedOldestAge}).");
                }
            }

            var deadLettered = Interlocked.Read(ref _deadLetteredSinceStart);
            data["dead_lettered_since_start"] = deadLettered;

            if (deadLettered >= _options.UnhealthyDeadLetterCount)
            {
                status = Worse(status, HealthStatus.Unhealthy);
                descriptions.Add($"{deadLettered} entries dead-lettered since start (unhealthy threshold {_options.UnhealthyDeadLetterCount}).");
            }
            else if (deadLettered >= _options.DegradedDeadLetterCount)
            {
                status = Worse(status, HealthStatus.Degraded);
                descriptions.Add($"{deadLettered} entries dead-lettered since start (degraded threshold {_options.DegradedDeadLetterCount}).");
            }

            if (_options.WorkerHeartbeatTimeout is { } heartbeatTimeout)
            {
                if (_workerHeartbeat is not null && _workerHeartbeat.LastPolledAtUtc.TryGetValue(_channelKey, out var lastPolledAtUtc))
                {
                    data["last_poll_utc"] = lastPolledAtUtc;
                    var sinceLastPoll = _timeProvider.GetUtcNow() - lastPolledAtUtc;

                    if (sinceLastPoll >= heartbeatTimeout)
                    {
                        status = Worse(status, HealthStatus.Unhealthy);
                        descriptions.Add($"Retry worker has not polled in {sinceLastPoll} (timeout {heartbeatTimeout}).");
                    }
                }
                else
                {
                    status = Worse(status, HealthStatus.Degraded);
                    descriptions.Add("Retry worker has not completed a single poll yet.");
                }
            }

            return new HealthCheckResult(status, descriptions.Count == 0 ? "Healthy." : string.Join(" ", descriptions), data: data);
        }

        private static HealthStatus Worse(HealthStatus a, HealthStatus b) => (HealthStatus)Math.Min((int)a, (int)b);

        public void Dispose() => _deadLetterListener.Dispose();
    }
}
