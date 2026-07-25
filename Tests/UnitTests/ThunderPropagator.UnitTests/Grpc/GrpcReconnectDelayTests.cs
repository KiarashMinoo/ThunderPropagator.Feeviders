using ThunderPropagator.Feeders.Grpc;

namespace ThunderPropagator.UnitTests.Grpc
{
    public class GrpcReconnectDelayTests
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
            var delay = GrpcReconnectDelay.Calculate(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(10),
                attempt);

            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), delay);
        }

        [Fact]
        public void Calculate_ShouldRejectMaximumBelowInitialDelay()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => GrpcReconnectDelay.Calculate(
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(1),
                1));
        }
    }
}
