using ThunderPropagator.Feeders.Kafka;

namespace ThunderPropagator.UnitTests.Kafka
{
    public class BlockingOperationRunnerTests
    {
        [Fact]
        public async Task RunAsync_ShouldExecuteOutsideTheThreadPool()
        {
            var isThreadPoolThread = await BlockingOperationRunner.RunAsync(
                () => Thread.CurrentThread.IsThreadPoolThread);

            Assert.False(isThreadPoolThread);
        }

        [Fact]
        public async Task RunAsync_ShouldNotInvokeOperationWhenAlreadyCancelled()
        {
            using var cancellationTokenSource = new CancellationTokenSource();
            await cancellationTokenSource.CancelAsync();
            var wasInvoked = false;

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                BlockingOperationRunner.RunAsync(() => wasInvoked = true, cancellationTokenSource.Token));

            Assert.False(wasInvoked);
        }
    }
}
