using ProtoBuf;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    public class FeederMessageSerializerTests
    {
        [Fact]
        public void SerializeToBytes_ShouldRoundTripProtobuf()
        {
            var serializer = CreateSerializer(SerializerType.Protobuf);
            var message = new TestProviderMessage { Value = "binary payload" };

            var bytes = serializer.SerializeToBytes(message);
            var result = bytes.FromProtobuf<TestProviderMessage>();

            Assert.Equal(message.Value, result.Value);
        }

        [Fact]
        public void Serialize_ShouldRoundTripProtobufBase64()
        {
            var serializer = CreateSerializer(SerializerType.Protobuf);
            var message = new TestProviderMessage { Value = "base64 payload" };

            var value = serializer.Serialize(message);
            var result = value.FromProtobufBase64<TestProviderMessage>();

            Assert.Equal(message.Value, result.Value);
        }

        [Fact]
        public void SerializeToBytes_ShouldRoundTripMessagePack()
        {
            var serializer = CreateSerializer(SerializerType.MessagePack);
            var message = CreateDictionaryMessage("binary payload");

            var bytes = serializer.SerializeToBytes(message);
            var result = bytes.FromMessagePack<TestProviderMessage>();

            Assert.Equal(message["Value"]?.ToString(), result["Value"]?.ToString());
        }

        [Fact]
        public void Serialize_ShouldRoundTripMessagePackBase64()
        {
            var serializer = CreateSerializer(SerializerType.MessagePack);
            var message = CreateDictionaryMessage("base64 payload");

            var value = serializer.Serialize(message);
            var result = value.FromMessagePackBase64<TestProviderMessage>();

            Assert.Equal(message["Value"]?.ToString(), result["Value"]?.ToString());
        }

        private static FeederMessageSerializer<TestProviderMessage, TestProviderConfiguration> CreateSerializer(SerializerType serializerType)
            => new(new TestProviderConfiguration { SerializerType = serializerType });

        private static TestProviderMessage CreateDictionaryMessage(string value)
        {
            var message = new TestProviderMessage();
            message.SetPayloadValue(value);
            return message;
        }

        [ProtoContract(IgnoreListHandling = true)]
        internal sealed class TestProviderMessage : FeederMessage
        {
            [ProtoMember(1)]
            public string Value { get; set; } = string.Empty;

            public void SetPayloadValue(string value) => SetValue(value, "Value");
        }

        private sealed class TestProviderConfiguration : AbstractProviderConfiguration;
    }
}
