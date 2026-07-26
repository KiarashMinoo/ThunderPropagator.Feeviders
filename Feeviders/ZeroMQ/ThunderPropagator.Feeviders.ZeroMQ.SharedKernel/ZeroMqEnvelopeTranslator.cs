using System.Diagnostics;
using NetMQ;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeviders.ZeroMQ.SharedKernel
{
    internal static class ZeroMqEnvelopeTranslator
    {
        public static NetMQMessage ToMessage(ZeroMqSocketPattern socketPattern, string? topic, byte[] payload)
        {
            var message = new NetMQMessage();

            if (socketPattern == ZeroMqSocketPattern.PubSub)
                message.Append(topic ?? string.Empty);

            message.Append(Activity.Current?.Context is { } activityContext ? activityContext.ToNJsonBase64() : string.Empty);
            message.Append(Baggage.Current.ToNJsonBase64());
            message.Append(payload);

            return message;
        }

        public static (TFeederMessage Message, ActivityContext? ActivityContext, Baggage? Baggage) FromMessage<TFeederMessage>(
            ZeroMqSocketPattern socketPattern,
            NetMQMessage message,
            FormatDeserializerInvoker formatDeserializerInvoker,
            SerializerType serializerType)
            where TFeederMessage : FeederMessage
        {
            var offset = socketPattern == ZeroMqSocketPattern.PubSub ? 1 : 0;

            var activityContextFrame = message[offset].ConvertToString();
            var baggageFrame = message[offset + 1].ConvertToString();
            var payload = message[offset + 2].ToByteArray(true);

            var feederMessage = formatDeserializerInvoker(serializerType).Deserialize<TFeederMessage>(payload)
                ?? throw new InvalidOperationException($"Failed to deserialize a {typeof(TFeederMessage).Name} from the received ZeroMQ message payload.");

            ActivityContext? activityContext = string.IsNullOrEmpty(activityContextFrame) ? null : activityContextFrame.FromNJsonBase64<ActivityContext>();
            Baggage? baggage = string.IsNullOrEmpty(baggageFrame) ? null : baggageFrame.FromNJsonBase64<Baggage>();

            return (feederMessage, activityContext, baggage);
        }
    }
}
