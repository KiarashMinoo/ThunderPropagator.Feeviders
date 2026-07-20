using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ThunderPropagator.Feeviders.Mqtt.SharedKernel
{
    internal static class MqttTelemetry
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.mqtt");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.mqtt");

        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.received",
            description: "Number of MQTT messages successfully received.");

        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.receive.failed",
            description: "Number of MQTT messages that failed to be received/processed.");

        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.mqtt.receive.duration",
            unit: "ms",
            description: "Duration of MQTT message receive processing.");

        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.published",
            description: "Number of MQTT messages successfully published.");

        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.mqtt.messages.publish.failed",
            description: "Number of MQTT messages that failed to be published.");

        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.mqtt.publish.duration",
            unit: "ms",
            description: "Duration of MQTT message publish operations.");
    }
}
