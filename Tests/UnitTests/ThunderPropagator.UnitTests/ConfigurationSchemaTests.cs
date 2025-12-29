using NJsonSchema.Generation;
using Xunit.Abstractions;
using NJsonSchema;

namespace ThunderPropagator.UnitTests
{
    public class ConfigurationSchemaTests
    {
        private readonly ITestOutputHelper _output;

        public ConfigurationSchemaTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [InlineData(typeof(ThunderPropagator.Feeders.WebApi.WebApiFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.WebApi.WebApiProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.TcpSocket.TcpSocketFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.TcpSocket.TcpSocketProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.UdpClient.UdpClientFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.UdpClient.UdpClientProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.Mqtt.MqttFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.Mqtt.MqttProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.NATS.NatsFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.NATS.NatsProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.Kafka.KafkaFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.Kafka.KafkaProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.RabbitMQ.RabbitMQFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.RabbitMQ.RabbitMQProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.ActiveMQ.ActiveMQFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.ActiveMQ.ActiveMQProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.Pulsar.PulsarFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.Pulsar.PulsarProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.RedisPubSub.RedisPubSubFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.RedisPubSub.RedisPubSubProviderConfiguration))]
        [InlineData(typeof(ThunderPropagator.Feeders.WebSocket.WebSocketFeederConfiguration))]
        [InlineData(typeof(ThunderPropagator.Providers.DotNet.WebSocket.WebSocketProviderConfiguration))]
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
