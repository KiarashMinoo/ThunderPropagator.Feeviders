using System.Diagnostics;
using NSubstitute;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Feeviders.Grpc.SharedKernel;
using ThunderPropagator.Feeviders.Grpc.SharedKernel.Protos;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.Grpc
{
    public class GrpcEnvelopeTranslatorTests
    {
        private sealed class TestFeederMessage : FeederMessage;

        [Fact]
        public void ToEnvelope_ShouldCarryTopicPayloadAndBaggageHeader()
        {
            var payload = "hello"u8.ToArray();

            var envelope = GrpcEnvelopeTranslator.ToEnvelope("orders", payload);

            Assert.Equal("orders", envelope.Topic);
            Assert.Equal(payload, envelope.Payload.ToByteArray());
            Assert.True(envelope.Headers.ContainsKey(nameof(Baggage)));
        }

        [Fact]
        public void ToEnvelope_ShouldCarryActivityContextHeader_WhenActivityIsCurrent()
        {
            using var activitySource = new ActivitySource(nameof(ToEnvelope_ShouldCarryActivityContextHeader_WhenActivityIsCurrent));
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = activitySource.StartActivity("test-activity");

            var envelope = GrpcEnvelopeTranslator.ToEnvelope("orders", []);

            Assert.NotNull(activity);
            Assert.True(envelope.Headers.ContainsKey(nameof(ActivityContext)));
            Assert.False(string.IsNullOrEmpty(envelope.Headers[nameof(ActivityContext)]));
        }

        [Fact]
        public void ToFeederMessage_ShouldDeserializePayloadUsingConfiguredSerializerType()
        {
            var expectedMessage = new TestFeederMessage();
            var deserializer = Substitute.For<IFormatDeserializer>();
            deserializer.Deserialize<TestFeederMessage>(Arg.Any<byte[]>()).Returns(expectedMessage);
            FormatDeserializerInvoker invoker = _ => deserializer;

            var envelope = new GrpcEnvelope { Topic = "orders", Payload = Google.Protobuf.ByteString.CopyFromUtf8("payload") };

            var (message, activityContext, baggage) = GrpcEnvelopeTranslator.ToFeederMessage<TestFeederMessage>(
                envelope, invoker, JsonFormatSerializer.Json);

            Assert.Same(expectedMessage, message);
            Assert.Null(activityContext);
            Assert.Null(baggage);
        }

        [Fact]
        public void ToFeederMessage_ShouldExtractActivityContextAndBaggageFromHeaders()
        {
            var deserializer = Substitute.For<IFormatDeserializer>();
            deserializer.Deserialize<TestFeederMessage>(Arg.Any<byte[]>()).Returns(new TestFeederMessage());
            FormatDeserializerInvoker invoker = _ => deserializer;

            var sourceContext = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
            var envelope = new GrpcEnvelope { Topic = "orders", Payload = Google.Protobuf.ByteString.Empty };
            envelope.Headers[nameof(ActivityContext)] = sourceContext.ToNJsonBase64();
            envelope.Headers[nameof(Baggage)] = Baggage.Current.ToNJsonBase64();

            var (_, activityContext, baggage) = GrpcEnvelopeTranslator.ToFeederMessage<TestFeederMessage>(
                envelope, invoker, JsonFormatSerializer.Json);

            Assert.NotNull(activityContext);
            Assert.NotNull(baggage);
        }
    }
}
