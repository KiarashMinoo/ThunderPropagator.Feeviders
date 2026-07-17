using ThunderPropagator.Feeders.ActiveMQ;

namespace ThunderPropagator.UnitTests
{
    public class ActiveMQMessageProcessorTests
    {
        [Fact]
        public async Task Enqueue_ShouldNotBlockWhileMessageIsProcessing()
        {
            var processingStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var processingCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var processor = new ActiveMQMessageProcessor<string>(
                async _ =>
                {
                    processingStarted.SetResult();
                    await processingCompletion.Task;
                },
                _ => { });

            processor.Enqueue("message");
            await processingStarted.Task;

            var processorCompletion = processor.CompleteAsync();
            Assert.False(processorCompletion.IsCompleted);

            processingCompletion.SetResult();
            await processorCompletion;
        }

        [Fact]
        public async Task Enqueue_ShouldObserveAndReportProcessingFailure()
        {
            var expectedException = new InvalidOperationException("processing failed");
            var errorReported = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            var processor = new ActiveMQMessageProcessor<string>(
                _ => Task.FromException(expectedException),
                exception => errorReported.SetResult(exception));

            processor.Enqueue("message");

            Assert.Same(expectedException, await errorReported.Task);
            await processor.CompleteAsync();
        }
    }
}
