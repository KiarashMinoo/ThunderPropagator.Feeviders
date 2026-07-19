using System.Reflection;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Feeders.Mqtt;

namespace ThunderPropagator.UnitTests.IsEnabledGuards
{
    public class MqttFeederIsEnabledTests
    {
        private static TestMqttFeederConfiguration CreateConfiguration(bool isEnabled) => new()
        {
            IsEnabled = isEnabled,
            Topic = "test-topic",
            ClientId = "test-client",
            Host = "127.0.0.1",
            Port = 1
        };

        [Fact]
        public async Task StartAsync_ShouldSkipBrokerConnection_WhenDisabled()
        {
            var feeder = new MqttFeeder<IChannel, TestMqttFeederMessage, TestMqttFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                CreateConfiguration(isEnabled: false),
                FeederTestHarness.CreateHandler<IChannel, TestMqttFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestMqttFeederMessage, TestMqttFeederConfiguration>());

            await InvokeStartAsync(feeder).WaitAsync(TimeSpan.FromSeconds(5));
        }

        [Fact]
        public async Task StartAsync_ShouldAttemptBrokerConnection_WhenEnabled()
        {
            var feeder = new MqttFeeder<IChannel, TestMqttFeederMessage, TestMqttFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                CreateConfiguration(isEnabled: true),
                FeederTestHarness.CreateHandler<IChannel, TestMqttFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestMqttFeederMessage, TestMqttFeederConfiguration>());

            // IFeeder.StartAsync's explicit interface implementation catches and logs exceptions
            // rather than rethrowing. Invoke the protected StartAsync directly so the connection
            // failure surfaces here.
            await Assert.ThrowsAnyAsync<Exception>(() =>
                InvokeStartAsync(feeder).WaitAsync(TimeSpan.FromSeconds(10)));
        }

        private static Task InvokeStartAsync(object feeder)
        {
            var method = feeder.GetType().GetMethod("StartAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
            return (Task)method.Invoke(feeder, [CancellationToken.None])!;
        }

        internal sealed class TestMqttFeederMessage : MqttFeederMessage;

        internal sealed class TestMqttFeederConfiguration : MqttFeederConfiguration;
    }
}
