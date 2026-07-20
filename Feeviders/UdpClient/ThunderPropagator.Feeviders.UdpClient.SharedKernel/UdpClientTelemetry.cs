using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ThunderPropagator.Feeviders.UdpClient.SharedKernel
{
    internal static class UdpClientTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.udpclient");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.udpclient");

        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.udpclient.messages.received",
            description: "Number of UDP datagrams successfully received.");

        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.udpclient.messages.receive.failed",
            description: "Number of UDP datagrams that failed to be received/processed.");

        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.udpclient.receive.duration",
            unit: "ms",
            description: "Duration of UDP datagram receive processing.");

        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.udpclient.messages.published",
            description: "Number of UDP datagrams successfully published.");

        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.udpclient.messages.publish.failed",
            description: "Number of UDP datagrams that failed to be published.");

        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.udpclient.publish.duration",
            unit: "ms",
            description: "Duration of UDP datagram publish operations.");
    }
}
