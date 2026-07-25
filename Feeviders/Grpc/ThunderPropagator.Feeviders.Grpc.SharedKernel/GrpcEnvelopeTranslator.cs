using System.Diagnostics;
using Google.Protobuf;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeviders.Grpc.SharedKernel.Protos;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeviders.Grpc.SharedKernel
{
    internal static class GrpcEnvelopeTranslator
    {
        public static GrpcEnvelope ToEnvelope(string topic, byte[] payload)
        {
            var envelope = new GrpcEnvelope
            {
                Topic = topic,
                Payload = ByteString.CopyFrom(payload)
            };

            if (Activity.Current?.Context is { } activityContext)
                envelope.Headers[nameof(ActivityContext)] = activityContext.ToNJsonBase64();

            envelope.Headers[nameof(Baggage)] = Baggage.Current.ToNJsonBase64();

            return envelope;
        }

        public static (TFeederMessage Message, ActivityContext? ActivityContext, Baggage? Baggage) ToFeederMessage<TFeederMessage>(
            GrpcEnvelope envelope,
            FormatDeserializerInvoker formatDeserializerInvoker,
            SerializerType serializerType)
            where TFeederMessage : FeederMessage
        {
            var message = formatDeserializerInvoker(serializerType).Deserialize<TFeederMessage>(envelope.Payload.ToByteArray())
                ?? throw new InvalidOperationException($"Failed to deserialize a {typeof(TFeederMessage).Name} from the received GrpcEnvelope payload.");

            ActivityContext? activityContext = null;
            if (envelope.Headers.TryGetValue(nameof(ActivityContext), out var activityContextStr))
                activityContext = activityContextStr.FromNJsonBase64<ActivityContext>();

            Baggage? baggage = null;
            if (envelope.Headers.TryGetValue(nameof(Baggage), out var baggageStr))
                baggage = baggageStr.FromNJsonBase64<Baggage>();

            return (message, activityContext, baggage);
        }
    }
}
