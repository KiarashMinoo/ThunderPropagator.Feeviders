using System.Diagnostics.Metrics;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    internal static class OutboxPurgeTelemetry
    {
        internal static readonly Meter Meter = new("thunderpropagator.providers.dotnet.outbox.purge");

        internal static readonly Counter<long> EntriesPurged = Meter.CreateCounter<long>(
            "thunderpropagator.providers.dotnet.outbox.purge.entries",
            description: "Number of terminal Outbox entries purged.");

        internal static readonly Counter<long> BatchesRun = Meter.CreateCounter<long>(
            "thunderpropagator.providers.dotnet.outbox.purge.batches",
            description: "Number of Outbox purge batches executed.");

        internal static readonly Histogram<double> BatchDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.providers.dotnet.outbox.purge.batch.duration",
            unit: "ms",
            description: "Duration of one IOutboxStore.PurgeAsync batch call.");
    }
}
