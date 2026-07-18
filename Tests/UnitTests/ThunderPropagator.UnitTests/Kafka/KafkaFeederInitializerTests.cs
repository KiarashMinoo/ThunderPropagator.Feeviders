using NSubstitute;
using ThunderPropagator.Feeders.Kafka;

namespace ThunderPropagator.UnitTests.Kafka
{
    public class KafkaFeederInitializerTests
    {
        [Fact]
        public void Initialize_ShouldDisposePartialResourcesWhenConsumerInitializationFails()
        {
            var consumer = Substitute.For<IDisposable>();
            var expectedException = new InvalidOperationException("subscribe failed");
            var schemaRegistryDisposed = false;

            var exception = Assert.Throws<InvalidOperationException>(() =>
                KafkaFeederInitializer.Initialize(
                    () => consumer,
                    _ => throw expectedException,
                    () => schemaRegistryDisposed = true));

            Assert.Same(expectedException, exception);
            consumer.Received(1).Dispose();
            Assert.True(schemaRegistryDisposed);
        }

        [Fact]
        public void Initialize_ShouldDisposeSchemaRegistryWhenConsumerCreationFails()
        {
            var expectedException = new InvalidOperationException("consumer build failed");
            var schemaRegistryDisposed = false;

            var exception = Assert.Throws<InvalidOperationException>(() =>
                KafkaFeederInitializer.Initialize<IDisposable>(
                    () => throw expectedException,
                    _ => { },
                    () => schemaRegistryDisposed = true));

            Assert.Same(expectedException, exception);
            Assert.True(schemaRegistryDisposed);
        }

        [Fact]
        public void Initialize_ShouldPreserveInitializationExceptionWhenCleanupFails()
        {
            var consumer = Substitute.For<IDisposable>();
            consumer.When(resource => resource.Dispose()).Do(_ => throw new InvalidOperationException("consumer dispose failed"));
            var expectedException = new InvalidOperationException("subscribe failed");

            var exception = Assert.Throws<InvalidOperationException>(() =>
                KafkaFeederInitializer.Initialize(
                    () => consumer,
                    _ => throw expectedException,
                    () => throw new InvalidOperationException("schema registry dispose failed")));

            Assert.Same(expectedException, exception);
        }
    }
}
