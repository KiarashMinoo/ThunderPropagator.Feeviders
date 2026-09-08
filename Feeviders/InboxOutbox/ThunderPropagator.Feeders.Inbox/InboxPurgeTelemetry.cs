using System.Diagnostics.Metrics;

namespace ThunderPropagator.Feeders.Inbox
{
    internal static class InboxPurgeTelemetry
    {
        internal static readonly Meter Meter = new("thunderpropagator.feeders.inbox.purge");

        internal static readonly Counter<long> EntriesPurged = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.purge.entries",
            description: "Number of terminal Inbox entries purged.");

        internal static readonly Counter<long> BatchesRun = Meter.CreateCounter<long>(
            "thunderpropagator.feeders.inbox.purge.batches",
            description: "Number of Inbox purge batches executed.");

        internal static readonly Histogram<double> BatchDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeders.inbox.purge.batch.duration",
            unit: "ms",
            description: "Duration of one IInboxStore.PurgeAsync batch call.");
    }
}
