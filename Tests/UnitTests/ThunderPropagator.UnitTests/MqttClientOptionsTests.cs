using MQTTnet.Formatter;
using ThunderPropagator.Feeders.Mqtt;

namespace ThunderPropagator.UnitTests
{
    public class MqttClientOptionsTests
    {
        [Fact]
        public void ToMqttClientOptions_ShouldApplyMaximumPacketSize()
        {
            const uint maximumPacketSize = 1_048_576;
            var configuration = new TestMqttFeederConfiguration
            {
                Host = "localhost",
                ClientId = "test-client",
                Topic = "test/topic",
                MaximumPacketSize = maximumPacketSize,
                ProtocolVersion = MqttProtocolVersion.V500,
                ReceiveMaximum = 1,
                Timeout = TimeSpan.FromSeconds(1)
            };

            var options = configuration.ToMqttClientOptions();

            Assert.Equal(maximumPacketSize, options.MaximumPacketSize);
        }

        private sealed class TestMqttFeederConfiguration : MqttFeederConfiguration;
    }
}
