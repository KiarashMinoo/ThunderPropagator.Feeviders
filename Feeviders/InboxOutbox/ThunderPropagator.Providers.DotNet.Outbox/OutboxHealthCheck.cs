using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Reports one Provider's Outbox health beyond raw store connectivity: backlog depth, oldest-pending
    /// age, dead-letter growth, and worker heartbeat - the signals needed to tell "the store is
    /// unavailable," "messages are piling up," and "the relay worker is stuck" apart from each other and
    /// from ordinary, healthy operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Register as a singleton.</b> This type taps <see cref="OutboxTelemetry.DeadLettered"/> via a
    /// <see cref="MeterListener"/> to accumulate a dead-letter count since construction - there is no
    /// store-level "count of dead-lettered entries" primitive cheap enough to poll without a full scan.
    /// Constructing a new instance per check would reset that accumulator to zero every time, making the
    /// dead-letter threshold meaningless - dispose it exactly once, when the health check itself is
    /// disposed (e.g. by the DI container at shutdown).
    /// </para>
    /// <para>
    /// <b>Never a full scan.</b> Backlog depth/oldest age come from <see cref="IOutboxStore.GetDepthAsync"/>/
    /// <see cref="IOutboxStore.GetOldestPendingAgeAsync"/>, both already store-native aggregate queries -
    /// this health check adds no scanning of its own on top of them.
    /// </para>
    /// <para>
    /// <b>Never exposes secrets.</b> <see cref="HealthCheckResult.Data"/> carries only counts, ages, a
    /// status string, and the Provider key itself - never a connection string (the store's own
    /// <see cref="IHealthCheck"/>, if any, is trusted to keep its own description free of one), a payload,
    /// or an <see cref="OutboxMessage.MessageId"/>.
    /// </para>
    /// </remarks>
    public sealed class OutboxHealthCheck : IHealthCheck, IDisposable
    {
        private readonly string _providerKey;
        private readonly IOutboxStore _store;
        private readonly OutboxHealthCheckOptions _options;
        private readonly IOutboxWorkerHeartbeat? _workerHeartbeat;
        private readonly TimeProvider _timeProvider;
        private readonly MeterListener _deadLetterListener;
        private long _deadLetteredSinceStart;

        /// <param name="providerKey">The Provider this instance reports on - matches <see cref="OutboxMessage.ProviderKey"/>.</param>
        /// <param name="store">The Provider's store. If it also implements <see cref="IHealthCheck"/>, that check's result gates everything else here - see <see cref="CheckHealthAsync"/>.</param>
        /// <param name="options">Thresholds. Defaults applied (and validated) when omitted.</param>
        /// <param name="workerHeartbeat">
        /// The <c>OutboxRelayWorker</c> polling this Provider, if any - omit only when this Provider
        /// deliberately has no relay worker wired up, in which case set
        /// <see cref="OutboxHealthCheckOptions.WorkerHeartbeatTimeout"/> to <see langword="null"/> too, or
        /// every check reports Degraded for a heartbeat that will never come.
        /// </param>
        /// <param name="timeProvider">Clock used for age/heartbeat comparisons. Defaults to <see cref="TimeProvider.System"/> - tests should supply a fake for deterministic threshold assertions.</param>
        /// <exception cref="ArgumentException"><paramref name="options"/> fails <see cref="OutboxHealthCheckOptions.Validate"/>.</exception>
        public OutboxHealthCheck(
            string providerKey,
            IOutboxStore store,
            OutboxHealthCheckOptions? options = null,
            IOutboxWorkerHeartbeat? workerHeartbeat = null,
            TimeProvider? timeProvider = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(providerKey);
            ArgumentNullException.ThrowIfNull(store);

            _providerKey = providerKey;
            _store = store;
            _options = options ?? new OutboxHealthCheckOptions();
            _options.Validate();
            _workerHeartbeat = workerHeartbeat;
            _timeProvider = timeProvider ?? TimeProvider.System;

            _deadLetterListener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument, OutboxTelemetry.DeadLettered))
                        listener.EnableMeasurementEvents(instrument);
                },
            };
            _deadLetterListener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == OutboxTelemetry.TagProvider && Equals(tag.Value, providerKey))
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
            var data = new Dictionary<string, object> { ["provider"] = _providerKey };

            if (_store is IHealthCheck storeHealthCheck)
            {
                HealthCheckResult storeResult;
                try
                {
                    storeResult = await storeHealthCheck.CheckHealthAsync(context, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    return HealthCheckResult.Unhealthy("Outbox store threw while checking connectivity.", exception, data);
                }

                if (storeResult.Status != HealthStatus.Healthy)
                {
                    data["store_status"] = storeResult.Status.ToString();
                    return new HealthCheckResult(storeResult.Status, storeResult.Description, storeResult.Exception, data);
                }
            }

            var status = HealthStatus.Healthy;
            var descriptions = new List<string>();

            int depth;
            TimeSpan? oldestPendingAge;
            try
            {
                depth = await _store.GetDepthAsync(null, cancellationToken).ConfigureAwait(false);
                oldestPendingAge = await _store.GetOldestPendingAgeAsync(null, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                return HealthCheckResult.Unhealthy("Outbox store threw while probing backlog depth/age.", exception, data);
            }

            data["backlog_count"] = depth;

            if (depth >= _options.UnhealthyBacklogCount)
            {
                status = Worse(status, HealthStatus.Unhealthy);
                descriptions.Add($"Backlog is {depth} (unhealthy threshold {_options.UnhealthyBacklogCount}).");
            }
            else if (depth >= _options.DegradedBacklogCount)
            {
                status = Worse(status, HealthStatus.Degraded);
                descriptions.Add($"Backlog is {depth} (degraded threshold {_options.DegradedBacklogCount}).");
            }

            if (oldestPendingAge is { } age)
            {
                data["oldest_age_seconds"] = age.TotalSeconds;

                if (age >= _options.UnhealthyOldestAge)
                {
                    status = Worse(status, HealthStatus.Unhealthy);
                    descriptions.Add($"Oldest pending entry is {age} old (unhealthy threshold {_options.UnhealthyOldestAge}).");
                }
                else if (age >= _options.DegradedOldestAge)
                {
                    status = Worse(status, HealthStatus.Degraded);
                    descriptions.Add($"Oldest pending entry is {age} old (degraded threshold {_options.DegradedOldestAge}).");
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
                if (_workerHeartbeat is not null && _workerHeartbeat.LastPolledAtUtc.TryGetValue(_providerKey, out var lastPolledAtUtc))
                {
                    data["last_poll_utc"] = lastPolledAtUtc;
                    var sinceLastPoll = _timeProvider.GetUtcNow() - lastPolledAtUtc;

                    if (sinceLastPoll >= heartbeatTimeout)
                    {
                        status = Worse(status, HealthStatus.Unhealthy);
                        descriptions.Add($"Relay worker has not polled in {sinceLastPoll} (timeout {heartbeatTimeout}).");
                    }
                }
                else
                {
                    status = Worse(status, HealthStatus.Degraded);
                    descriptions.Add("Relay worker has not completed a single poll yet.");
                }
            }

            return new HealthCheckResult(status, descriptions.Count == 0 ? "Healthy." : string.Join(" ", descriptions), data: data);
        }

        private static HealthStatus Worse(HealthStatus a, HealthStatus b) => (HealthStatus)Math.Min((int)a, (int)b);

        public void Dispose() => _deadLetterListener.Dispose();
    }
}
