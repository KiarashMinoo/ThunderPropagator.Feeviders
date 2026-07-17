namespace ThunderPropagator.Feeders.RedisPubSub
{
    internal static class RedisPubSubMessageHandler
    {
        public static async Task ProcessAsync<TMessage>(
            TMessage message,
            Func<TMessage, Task> processMessageAsync,
            Action<Exception> handleError)
        {
            try
            {
                await processMessageAsync(message).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                handleError(exception);
            }
        }
    }
}
