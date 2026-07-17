namespace ThunderPropagator.Feeders.ActiveMQ
{
    internal static class ActiveMQMessageHandler
    {
        public static void Process<TMessage>(
            TMessage message,
            Func<TMessage, Task> processMessageAsync,
            Action<Exception> handleError)
        {
            try
            {
                Task.Run(() => processMessageAsync(message)).GetAwaiter().GetResult();
            }
            catch (Exception exception)
            {
                handleError(exception);
            }
        }
    }
}
