using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ThunderPropagator.Feeviders.ActiveMQ.SharedKernel
{
    internal static class ActiveMQTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.activemq");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.activemq");

        internal static readonly Counter<long> MessagesReceived =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.activemq.messages.received");

        internal static readonly Counter<long> MessagesReceiveFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.activemq.messages.receive.failed");

        internal static readonly Histogram<double> ReceiveDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.activemq.receive.duration", unit: "ms");

        internal static readonly Counter<long> MessagesPublished =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.activemq.messages.published");

        internal static readonly Counter<long> MessagesPublishFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.activemq.messages.publish.failed");

        internal static readonly Histogram<double> PublishDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.activemq.publish.duration", unit: "ms");
    }
}
