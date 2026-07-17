using ThunderPropagator.Feeders.ActiveMQ;

namespace ThunderPropagator.UnitTests
{
    public class ActiveMQMessageHandlerTests
    {
        [Fact]
        public async Task Process_ShouldWaitForMessageProcessing()
        {
            var processingStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var processingCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var handlerTask = Task.Run(() => ActiveMQMessageHandler.Process(
                "message",
                async _ =>
                {
                    processingStarted.SetResult();
                    await processingCompletion.Task;
                },
                _ => { }));

            await processingStarted.Task;
            Assert.False(handlerTask.IsCompleted);

            processingCompletion.SetResult();
            await handlerTask;
        }

        [Fact]
        public void Process_ShouldObserveAndReportProcessingFailure()
        {
            var expectedException = new InvalidOperationException("processing failed");
            Exception? observedException = null;

            ActiveMQMessageHandler.Process(
                "message",
                _ => Task.FromException(expectedException),
                exception => observedException = exception);

            Assert.Same(expectedException, observedException);
        }
    }
}
