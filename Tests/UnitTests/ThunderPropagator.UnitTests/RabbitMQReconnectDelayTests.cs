using ThunderPropagator.Feeders.RabbitMQ;

namespace ThunderPropagator.UnitTests
{
    public class RabbitMQReconnectDelayTests
    {
        [Theory]
        [InlineData(1, 1)]
        [InlineData(2, 2)]
        [InlineData(3, 4)]
        [InlineData(4, 8)]
        [InlineData(5, 10)]
        [InlineData(20, 10)]
        public void Calculate_ShouldExponentiallyIncreaseUntilMaximum(int attempt, int expectedSeconds)
        {
            var delay = RabbitMQReconnectDelay.Calculate(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(10),
                attempt);

            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), delay);
        }

        [Fact]
        public void Calculate_ShouldRejectMaximumBelowInitialDelay()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => RabbitMQReconnectDelay.Calculate(
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(1),
                1));
        }
    }
}
