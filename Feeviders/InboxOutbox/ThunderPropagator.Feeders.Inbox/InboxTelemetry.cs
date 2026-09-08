using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Inbox metrics shared by every call site that mutates an <see cref="InboxMessage"/>'s lifecycle -
    /// the live receive path (<c>InboxReceiveCoordinator</c>, in the Feeders SharedKernel) and the retry
    /// path (<see cref="InboxRetryWorker"/>, here) both record into these same instruments, so a
    /// dashboard never has to reconcile two separate metric families for what is conceptually one Inbox.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Tags are bounded by design.</b> Every tag value here is one of: a channel key (bounded by how
    /// many channels are configured), a store backend name, a failure category (a fixed 5-value enum), an
    /// operation name (a fixed, small set of method names), or an outcome (success/error). Nothing here
    /// ever tags with <see cref="InboxMessage.MessageId"/>, a partition key, or any other caller-supplied,
    /// effectively-unbounded value - unbounded tags create unbounded time-series cardinality in whatever
    /// backend scrapes these (Prometheus, OTLP, etc.), which is the exact failure mode bounded-tag design
    /// exists to avoid.
    /// </para>
    /// <para>
    /// <b>Naming.</b> Instrument names use the dotted OpenTelemetry convention (matching every other
    /// <c>*Telemetry</c> class in this repo) - an OTLP-to-Prometheus exporter renders dots as underscores
    /// automatically, so no separate Prometheus-specific naming is needed. Every duration
    /// <see cref="Histogram{T}"/> uses <c>ms</c> (milliseconds) as its unit.
    /// </para>
    /// <para>
    /// <b>Never blocks a store.</b> Every instrument here is recorded with a plain, synchronous
    /// <c>Add</c>/<c>Record</c> call carrying only already-computed values (a duration already measured, a
    /// count already known) - nothing here performs I/O, allocation-heavy work, or awaits anything, so
    /// recording a metric can never itself become the slow part of a store operation.
    /// </para>
    /// </remarks>
    internal static class InboxTelemetry
    {
        internal const string TagChannel = "channel";
        internal const string TagBackend = "backend";
        internal const string TagCategory = "category";
        internal const string TagOperation = "operation";
        internal const string TagOutcome = "outcome";

        internal static readonly Meter Meter = new("thunderpropagator.feeders.inbox");

        /// <summary>
        /// Shared by both the live receive path (<c>InboxReceiveCoordinator</c>, in the Feeders
        /// SharedKernel) and the retry path (<see cref="InboxRetryWorker"/>, here) - see
        /// <see cref="InboxTraceContext"/> for how a retry activity links back to the original receive's
        /// trace instead of pretending to be its synchronous child.
        /// </summary>
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeders.inbox");

        /// <summary>A brand-new message was durably claimed for the first time (not a retry reclaim, not a duplicate).</summary>
        internal static readonly Counter<long> Received = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.received",
            description: "Number of Inbox messages newly and durably claimed.");

        /// <summary>A redelivery of a message already Processed or DeadLettered - see <see cref="TagCategory"/>-less <c>existing_status</c> tag.</summary>
        internal static readonly Counter<long> Duplicates = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.duplicates",
            description: "Number of Inbox message deliveries recognized as duplicates of an already-terminal entry.");

        internal static readonly Counter<long> Processed = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.processed",
            description: "Number of Inbox entries that reached Processed.");

        /// <summary>A transient, retry-eligible failure - not a dead-letter.</summary>
        internal static readonly Counter<long> Failed = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.failed",
            description: "Number of Inbox processing attempts that failed and were scheduled for retry.");

        internal static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.dead_lettered",
            description: "Number of Inbox entries newly dead-lettered.");

        /// <summary><see cref="InboxClaimOutcome.ClaimedByAnotherOwner"/> - a live lease held by someone else at the moment of claim.</summary>
        internal static readonly Counter<long> ClaimContention = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.claim_contention",
            description: "Number of Inbox claim attempts that lost a race to an already-held lease.");

        internal static readonly Histogram<double> RetryDelay = Meter.CreateHistogram<double>(
            "thunderpropagator.feeders.inbox.retry_delay",
            unit: "ms",
            description: "Computed backoff delay before an Inbox entry's next retry attempt.");

        internal static readonly Histogram<double> StoreOperationDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeders.inbox.store_operation.duration",
            unit: "ms",
            description: "Duration of one IInboxStore operation call.");
    }
}
