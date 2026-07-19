using System.Reflection;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Feeders.RabbitMQ;

namespace ThunderPropagator.UnitTests.IsEnabledGuards
{
    public class RabbitMQFeederIsEnabledTests
    {
        private static TestRabbitMQFeederConfiguration CreateConfiguration(bool isEnabled) => new()
        {
            IsEnabled = isEnabled,
            Queue = "test-queue",
            HostName = "127.0.0.1",
            Port = 1,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(1),
            AutomaticRecoveryEnabled = false
        };

        [Fact]
        public async Task StartAsync_ShouldSkipBrokerConnection_WhenDisabled()
        {
            var feeder = new RabbitMQFeeder<IChannel, TestRabbitMQFeederMessage, TestRabbitMQFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                CreateConfiguration(isEnabled: false),
                FeederTestHarness.CreateHandler<IChannel, TestRabbitMQFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestRabbitMQFeederMessage, TestRabbitMQFeederConfiguration>());

            await InvokeStartAsync(feeder).WaitAsync(TimeSpan.FromSeconds(5));
        }

        [Fact]
        public async Task StartAsync_ShouldAttemptBrokerConnection_WhenEnabled()
        {
            var feeder = new RabbitMQFeeder<IChannel, TestRabbitMQFeederMessage, TestRabbitMQFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                CreateConfiguration(isEnabled: true),
                FeederTestHarness.CreateHandler<IChannel, TestRabbitMQFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestRabbitMQFeederMessage, TestRabbitMQFeederConfiguration>());

            // IFeeder.StartAsync's explicit interface implementation catches and logs exceptions
            // rather than rethrowing (so one feeder's startup failure doesn't crash the host).
            // Invoke the protected StartAsync directly so the connection failure surfaces here.
            await Assert.ThrowsAnyAsync<Exception>(() =>
                InvokeStartAsync(feeder).WaitAsync(TimeSpan.FromSeconds(10)));
        }

        private static Task InvokeStartAsync(object feeder)
        {
            var method = feeder.GetType().GetMethod("StartAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
            return (Task)method.Invoke(feeder, [CancellationToken.None])!;
        }

        internal sealed class TestRabbitMQFeederMessage : RabbitMQFeederMessage;

        internal sealed class TestRabbitMQFeederConfiguration : RabbitMQFeederConfiguration;
    }
}
