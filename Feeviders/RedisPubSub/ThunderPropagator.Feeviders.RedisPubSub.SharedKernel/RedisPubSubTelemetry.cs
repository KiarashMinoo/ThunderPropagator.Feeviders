using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ThunderPropagator.Feeviders.RedisPubSub.SharedKernel
{
    internal static class RedisPubSubTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.redispubsub");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.redispubsub");

        internal static readonly Counter<long> MessagesReceived =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.received");

        internal static readonly Counter<long> MessagesReceiveFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.receive.failed");

        internal static readonly Histogram<double> ReceiveDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.redispubsub.receive.duration", unit: "ms");

        internal static readonly Counter<long> MessagesPublished =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.published");

        internal static readonly Counter<long> MessagesPublishFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.redispubsub.messages.publish.failed");

        internal static readonly Histogram<double> PublishDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.redispubsub.publish.duration", unit: "ms");
    }
}
