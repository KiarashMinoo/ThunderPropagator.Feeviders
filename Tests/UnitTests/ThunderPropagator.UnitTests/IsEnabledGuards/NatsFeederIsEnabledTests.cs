using System.Reflection;
using NATS.Client.JetStream.Models;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Feeders.NATS;
using ThunderPropagator.Feeviders.NATS.SharedKernel;

namespace ThunderPropagator.UnitTests.IsEnabledGuards
{
    public class NatsFeederIsEnabledTests
    {
        private static TestNatsFeederConfiguration CreateJetStreamConfiguration(bool isEnabled) => new()
        {
            IsEnabled = isEnabled,
            Url = "nats://127.0.0.1:1",
            ConnectTimeout = TimeSpan.FromSeconds(1),
            RequestTimeout = TimeSpan.FromSeconds(1),
            CommandTimeout = TimeSpan.FromSeconds(1),
            MaxReconnectRetry = 0,
            MessagingType = MessagingType.JetStream,
            StreamName = "test-stream",
            ConsumerConfig = new ConsumerConfig()
        };

        [Fact]
        public async Task StartingAsync_ShouldSkipJetStreamInitialization_WhenDisabled()
        {
            var feeder = new NatsFeeder<IChannel, TestNatsFeederMessage, TestNatsFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                CreateJetStreamConfiguration(isEnabled: false),
                FeederTestHarness.CreateHandler<IChannel, TestNatsFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestNatsFeederMessage, TestNatsFeederConfiguration>());

            await InvokeStartingAsync(feeder).WaitAsync(TimeSpan.FromSeconds(5));

            Assert.False(GetIsJetStreamReady(feeder));
            Assert.Null(GetJetStreamInitializationException(feeder));
        }

        [Fact]
        public async Task StartingAsync_ShouldAttemptJetStreamInitialization_WhenEnabled()
        {
            var feeder = new NatsFeeder<IChannel, TestNatsFeederMessage, TestNatsFeederConfiguration>(
                FeederTestHarness.CreateChannel(),
                CreateJetStreamConfiguration(isEnabled: true),
                FeederTestHarness.CreateHandler<IChannel, TestNatsFeederMessage>(),
                FeederTestHarness.CreateServiceProvider<TestNatsFeederMessage, TestNatsFeederConfiguration>());

            // NatsFeeder implements IFeature, and IFeeder.StartingAsync's explicit interface
            // implementation gates on a license check before ever calling the protected override.
            // Invoke the protected StartingAsync directly via reflection to test the IsEnabled
            // guard itself, independent of licensing. Calling it directly also bypasses the
            // catch-log-swallow wrapper, so the connection failure surfaces as a real exception here.
            await Assert.ThrowsAnyAsync<Exception>(() => InvokeStartingAsync(feeder).WaitAsync(TimeSpan.FromSeconds(10)));

            Assert.False(GetIsJetStreamReady(feeder));
            Assert.NotNull(GetJetStreamInitializationException(feeder));
        }

        private static Task InvokeStartingAsync(object feeder)
        {
            var method = feeder.GetType().GetMethod("StartingAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
            return (Task)method.Invoke(feeder, [CancellationToken.None])!;
        }

        private static bool GetIsJetStreamReady(object feeder)
            => (bool)feeder.GetType().GetField("_isJetStreamReady", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(feeder)!;

        private static Exception? GetJetStreamInitializationException(object feeder)
            => (Exception?)feeder.GetType().GetField("_jetStreamInitializationException", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(feeder);

        internal sealed class TestNatsFeederMessage : NatsFeederMessage;

        internal sealed class TestNatsFeederConfiguration : NatsFeederConfiguration;
    }
}
