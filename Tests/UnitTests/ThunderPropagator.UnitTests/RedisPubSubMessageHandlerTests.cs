using ThunderPropagator.Feeders.RedisPubSub;

namespace ThunderPropagator.UnitTests
{
    public class RedisPubSubMessageHandlerTests
    {
        [Fact]
        public async Task ProcessAsync_ShouldAwaitMessageProcessing()
        {
            var processingCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handlerTask = RedisPubSubMessageHandler.ProcessAsync(
                "message",
                async _ => await processingCompletion.Task,
                _ => { });

            Assert.False(handlerTask.IsCompleted);

            processingCompletion.SetResult();
            await handlerTask;
        }

        [Fact]
        public async Task ProcessAsync_ShouldObserveAndReportProcessingFailure()
        {
            var expectedException = new InvalidOperationException("processing failed");
            Exception? observedException = null;

            await RedisPubSubMessageHandler.ProcessAsync(
                "message",
                _ => Task.FromException(expectedException),
                exception => observedException = exception);

            Assert.Same(expectedException, observedException);
        }
    }
}
