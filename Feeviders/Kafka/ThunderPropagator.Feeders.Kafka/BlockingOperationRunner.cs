namespace ThunderPropagator.Feeders.Kafka
{
    internal static class BlockingOperationRunner
    {
        internal static Task<TResult> RunAsync<TResult>(Func<TResult> operation, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(operation);

            return Task.Factory.StartNew(
                operation,
                cancellationToken,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);
        }
    }
}
