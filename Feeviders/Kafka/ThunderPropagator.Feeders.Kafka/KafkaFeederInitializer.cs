namespace ThunderPropagator.Feeders.Kafka
{
    internal static class KafkaFeederInitializer
    {
        public static TConsumer Initialize<TConsumer>(
            Func<TConsumer> createConsumer,
            Action<TConsumer> initializeConsumer,
            Action disposeSchemaRegistry)
            where TConsumer : class, IDisposable
        {
            TConsumer? consumer = null;

            try
            {
                consumer = createConsumer();
                initializeConsumer(consumer);
                return consumer;
            }
            catch
            {
                if (consumer is not null)
                    TryCleanup(consumer.Dispose);
                TryCleanup(disposeSchemaRegistry);
                throw;
            }
        }

        private static void TryCleanup(Action? cleanup)
        {
            try
            {
                cleanup?.Invoke();
            }
            catch
            {
                // Preserve the initialization exception that triggered cleanup.
            }
        }
    }
}
