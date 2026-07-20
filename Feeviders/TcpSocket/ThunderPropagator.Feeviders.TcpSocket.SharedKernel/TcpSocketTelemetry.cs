using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ThunderPropagator.Feeviders.TcpSocket.SharedKernel
{
    internal static class TcpSocketTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.tcpsocket");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.tcpsocket");

        internal static readonly Counter<long> MessagesReceived =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.tcpsocket.messages.received");

        internal static readonly Counter<long> MessagesReceiveFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.tcpsocket.messages.receive.failed");

        internal static readonly Histogram<double> ReceiveDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.tcpsocket.receive.duration", unit: "ms");

        internal static readonly Counter<long> MessagesPublished =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.tcpsocket.messages.published");

        internal static readonly Counter<long> MessagesPublishFailed =
            Meter.CreateCounter<long>("thunderpropagator.feeviders.tcpsocket.messages.publish.failed");

        internal static readonly Histogram<double> PublishDuration =
            Meter.CreateHistogram<double>("thunderpropagator.feeviders.tcpsocket.publish.duration", unit: "ms");
    }
}
