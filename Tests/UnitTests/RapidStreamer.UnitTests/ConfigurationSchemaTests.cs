using NJsonSchema.Generation;
using Xunit.Abstractions;
using NJsonSchema;

namespace RapidStreamer.UnitTests
{
    public class ConfigurationSchemaTests
    {
        private readonly ITestOutputHelper _output;

        public ConfigurationSchemaTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [InlineData(typeof(RapidStreamer.Feeders.WebApi.WebApiFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.WebApi.WebApiProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.TcpSocket.TcpSocketFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.TcpSocket.TcpSocketProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.UdpClient.UdpClientFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.UdpClient.UdpClientProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.Mqtt.MqttFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.Mqtt.MqttProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.NATS.NatsFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.NATS.NatsProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.Kafka.KafkaFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.Kafka.KafkaProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.RabbitMQ.RabbitMQFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.RabbitMQ.RabbitMQProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.ActiveMQ.ActiveMQFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.ActiveMQ.ActiveMQProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.Pulsar.PulsarFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.Pulsar.PulsarProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.RedisPubSub.RedisPubSubFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.RedisPubSub.RedisPubSubProviderConfiguration))]
        [InlineData(typeof(RapidStreamer.Feeders.WebSocket.WebSocketFeederConfiguration))]
        [InlineData(typeof(RapidStreamer.Providers.DotNet.WebSocket.WebSocketProviderConfiguration))]
        public void JsonSchema_Should_Generate_For_Configuration_Types(Type configurationType)
        {
            // Arrange
            Exception? error = null;
            JsonSchema? schema = null;

            try
            {
                schema = JsonSchema.FromType(configurationType, new SystemTextJsonSchemaGeneratorSettings
                {
                    DefaultReferenceTypeNullHandling = ReferenceTypeNullHandling.Null
                });
            }
            catch (Exception ex)
            {
                error = ex;
                _output.WriteLine(ex.ToString());
            }

            // Assert
            Assert.Null(error);
            Assert.NotNull(schema);
            Assert.True(schema.Definitions.Count >= 0); // simple smoke assertion
        }
    }
}
