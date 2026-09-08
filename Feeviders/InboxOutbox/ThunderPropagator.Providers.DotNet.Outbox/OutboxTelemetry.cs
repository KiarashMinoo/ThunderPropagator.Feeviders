using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Outbox metrics shared by every call site that mutates an <see cref="OutboxMessage"/>'s lifecycle -
    /// enqueue (<see cref="NonTransactionalOutboxUnitOfWork"/>/<c>EfCoreOutboxUnitOfWork</c>) and relay
    /// (<c>OutboxRelayWorker</c>, in the Providers SharedKernel) both record into these same instruments.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Tags are bounded by design.</b> Every tag value here is one of: a Provider key (bounded by how
    /// many Providers are configured), a store backend name, a failure category (a fixed 5-value enum),
    /// an operation name (a fixed, small set of method names), or an outcome (success/error). Nothing
    /// here ever tags with <see cref="OutboxMessage.MessageId"/>, a partition key, or any other
    /// caller-supplied, effectively-unbounded value.
    /// </para>
    /// <para>
    /// <b>Naming.</b> Instrument names use the dotted OpenTelemetry convention (matching every other
    /// <c>*Telemetry</c> class in this repo) - an OTLP-to-Prometheus exporter renders dots as underscores
    /// automatically. Every duration <see cref="Histogram{T}"/> uses <c>ms</c>; the age gauge uses
    /// <c>s</c> (seconds).
    /// </para>
    /// <para>
    /// <b>Never blocks a store.</b> The counters/histograms are recorded with plain, synchronous
    /// <c>Add</c>/<c>Record</c> calls carrying only already-computed values. The two
    /// <see cref="ObservableGauge{T}"/>s (<see cref="PendingDepth"/>/<see cref="OldestPendingAgeSeconds"/>)
    /// are the one place this would otherwise risk blocking - their true values
    /// (<see cref="IOutboxStore.GetDepthAsync"/>/<see cref="IOutboxStore.GetOldestPendingAgeAsync"/>) are
    /// async store calls, and an <see cref="ObservableGauge{T}"/> callback must be synchronous. So
    /// <c>OutboxRelayWorker</c>'s own polling loop calls those async methods itself (already polling
    /// anyway) and publishes the result into <see cref="ReportDepthSnapshot"/>; the gauge callbacks below
    /// only ever read that already-computed cached snapshot, never call the store themselves.
    /// </para>
    /// </remarks>
    internal static class OutboxTelemetry
    {
        internal const string TagProvider = "provider";
        internal const string TagBackend = "backend";
        internal const string TagCategory = "category";
        internal const string TagOperation = "operation";
        internal const string TagOutcome = "outcome";

        internal static readonly Meter Meter = new("thunderpropagator.providers.dotnet.outbox");

        /// <summary>
        /// Shared by both enqueue (<see cref="NonTransactionalOutboxUnitOfWork"/>/<c>EfCoreOutboxUnitOfWork</c>)
        /// and relay (<c>OutboxRelayWorker</c>, in the Providers SharedKernel) - see
        /// <see cref="OutboxTraceContext"/> for how a relay activity links back to the enqueuing trace
        /// instead of pretending to be its synchronous child.
        /// </summary>
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.providers.dotnet.outbox");

        internal static readonly Counter<long> Enqueued = Meter.CreateCounter<long>(
            "thunderpropagator.providers.dotnet.outbox.enqueued",
            description: "Number of Outbox messages durably enqueued.");

        internal static readonly Counter<long> Published = Meter.CreateCounter<long>(
            "thunderpropagator.providers.dotnet.outbox.published",
            description: "Number of Outbox entries that reached Published.");

        /// <summary>A transient, retry-eligible publish failure - not a dead-letter.</summary>
        internal static readonly Counter<long> Failed = Meter.CreateCounter<long>(
            "thunderpropagator.providers.dotnet.outbox.failed",
            description: "Number of Outbox publish attempts that failed and were scheduled for retry.");

        internal static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>(
            "thunderpropagator.providers.dotnet.outbox.dead_lettered",
            description: "Number of Outbox entries newly dead-lettered.");

        internal static readonly Histogram<double> RelayDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.providers.dotnet.outbox.relay.duration",
            unit: "ms",
            description: "Duration of one Outbox entry's publish attempt, from claim to terminal/retry outcome.");

        internal static readonly Histogram<double> StoreOperationDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.providers.dotnet.outbox.store_operation.duration",
            unit: "ms",
            description: "Duration of one IOutboxStore operation call.");

        private static readonly ConcurrentDictionary<string, (long Depth, double? OldestAgeSeconds)> DepthSnapshots = new(StringComparer.Ordinal);

        internal static readonly ObservableGauge<long> PendingDepth = Meter.CreateObservableGauge(
            "thunderpropagator.providers.dotnet.outbox.pending.depth",
            ObservePendingDepth,
            description: "Most recently observed Pending+backlog depth, per Provider.");

        internal static readonly ObservableGauge<double> OldestPendingAgeSeconds = Meter.CreateObservableGauge(
            "thunderpropagator.providers.dotnet.outbox.pending.oldest_age",
            ObserveOldestPendingAge,
            unit: "s",
            description: "Age of the oldest Pending entry, per Provider, as of the most recent poll. Absent when nothing is pending.");

        /// <summary>
        /// Called once per poll by <c>OutboxRelayWorker</c> with the result of its own
        /// <see cref="IOutboxStore.GetDepthAsync"/>/<see cref="IOutboxStore.GetOldestPendingAgeAsync"/>
        /// calls, so the gauges above have something to read without ever calling the store themselves.
        /// </summary>
        internal static void ReportDepthSnapshot(string providerKey, long depth, TimeSpan? oldestPendingAge) =>
            DepthSnapshots[providerKey] = (depth, oldestPendingAge?.TotalSeconds);

        private static IEnumerable<Measurement<long>> ObservePendingDepth() =>
            DepthSnapshots.Select(pair => new Measurement<long>(pair.Value.Depth, new KeyValuePair<string, object?>(TagProvider, pair.Key)));

        private static IEnumerable<Measurement<double>> ObserveOldestPendingAge() =>
            DepthSnapshots
                .Where(pair => pair.Value.OldestAgeSeconds is not null)
                .Select(pair => new Measurement<double>(pair.Value.OldestAgeSeconds!.Value, new KeyValuePair<string, object?>(TagProvider, pair.Key)));
    }
}
