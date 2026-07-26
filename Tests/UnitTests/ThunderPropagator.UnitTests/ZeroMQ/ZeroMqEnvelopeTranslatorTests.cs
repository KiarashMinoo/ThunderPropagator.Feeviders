using System.Diagnostics;
using NetMQ;
using NSubstitute;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.ZeroMQ
{
    public class ZeroMqEnvelopeTranslatorTests
    {
        private sealed class TestFeederMessage : FeederMessage;

        [Fact]
        public void ToMessage_PubSub_ShouldPrependTopicFrameAndCarryPayload()
        {
            var payload = "hello"u8.ToArray();

            var message = ZeroMqEnvelopeTranslator.ToMessage(ZeroMqSocketPattern.PubSub, "orders", payload);

            Assert.Equal(4, message.FrameCount);
            Assert.Equal("orders", message[0].ConvertToString());
            Assert.Equal(payload, message[3].ToByteArray(true));
        }

        [Fact]
        public void ToMessage_PushPull_ShouldNotIncludeATopicFrame()
        {
            var payload = "hello"u8.ToArray();

            var message = ZeroMqEnvelopeTranslator.ToMessage(ZeroMqSocketPattern.PushPull, "ignored-topic", payload);

            Assert.Equal(3, message.FrameCount);
            Assert.Equal(payload, message[2].ToByteArray(true));
        }

        [Fact]
        public void ToMessage_ShouldAlwaysCarryABaggageHeaderFrame()
        {
            var message = ZeroMqEnvelopeTranslator.ToMessage(ZeroMqSocketPattern.PushPull, null, []);

            Assert.False(string.IsNullOrEmpty(message[1].ConvertToString()));
        }

        [Fact]
        public void ToMessage_ShouldCarryAnActivityContextHeader_WhenActivityIsCurrent()
        {
            using var activitySource = new ActivitySource(nameof(ToMessage_ShouldCarryAnActivityContextHeader_WhenActivityIsCurrent));
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            };
            ActivitySource.AddActivityListener(listener);

            using var activity = activitySource.StartActivity("test-activity");

            var message = ZeroMqEnvelopeTranslator.ToMessage(ZeroMqSocketPattern.PushPull, null, []);

            Assert.NotNull(activity);
            Assert.False(string.IsNullOrEmpty(message[0].ConvertToString()));
        }

        [Fact]
        public void FromMessage_PubSub_ShouldSkipTopicFrameAndDeserializePayload()
        {
            var expectedMessage = new TestFeederMessage();
            var deserializer = Substitute.For<IFormatDeserializer>();
            deserializer.Deserialize<TestFeederMessage>(Arg.Any<byte[]>()).Returns(expectedMessage);
            FormatDeserializerInvoker invoker = _ => deserializer;

            var netMqMessage = ZeroMqEnvelopeTranslator.ToMessage(ZeroMqSocketPattern.PubSub, "orders", "payload"u8.ToArray());

            var (message, activityContext, baggage) = ZeroMqEnvelopeTranslator.FromMessage<TestFeederMessage>(
                ZeroMqSocketPattern.PubSub, netMqMessage, invoker, JsonFormatSerializer.Json);

            Assert.Same(expectedMessage, message);
            Assert.Null(activityContext);
            Assert.NotNull(baggage);
        }

        [Fact]
        public void FromMessage_PushPull_ShouldDeserializePayloadWithoutATopicFrame()
        {
            var expectedMessage = new TestFeederMessage();
            var deserializer = Substitute.For<IFormatDeserializer>();
            deserializer.Deserialize<TestFeederMessage>(Arg.Any<byte[]>()).Returns(expectedMessage);
            FormatDeserializerInvoker invoker = _ => deserializer;

            var netMqMessage = ZeroMqEnvelopeTranslator.ToMessage(ZeroMqSocketPattern.PushPull, null, "payload"u8.ToArray());

            var (message, _, _) = ZeroMqEnvelopeTranslator.FromMessage<TestFeederMessage>(
                ZeroMqSocketPattern.PushPull, netMqMessage, invoker, JsonFormatSerializer.Json);

            Assert.Same(expectedMessage, message);
        }

        [Fact]
        public void FromMessage_ShouldExtractActivityContextAndBaggageFromHeaders()
        {
            var deserializer = Substitute.For<IFormatDeserializer>();
            deserializer.Deserialize<TestFeederMessage>(Arg.Any<byte[]>()).Returns(new TestFeederMessage());
            FormatDeserializerInvoker invoker = _ => deserializer;

            var sourceContext = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
            var netMqMessage = new NetMQMessage();
            netMqMessage.Append(sourceContext.ToNJsonBase64());
            netMqMessage.Append(Baggage.Current.ToNJsonBase64());
            netMqMessage.Append("payload"u8.ToArray());

            var (_, activityContext, baggage) = ZeroMqEnvelopeTranslator.FromMessage<TestFeederMessage>(
                ZeroMqSocketPattern.PushPull, netMqMessage, invoker, JsonFormatSerializer.Json);

            Assert.NotNull(activityContext);
            Assert.NotNull(baggage);
        }
    }
}
