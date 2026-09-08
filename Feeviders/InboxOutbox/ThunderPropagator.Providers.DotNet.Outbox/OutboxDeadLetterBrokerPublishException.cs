namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Thrown by <see cref="OutboxBrokerDeadLetterHandler"/> when every bounded publish attempt to the broker DLQ fails.</summary>
    public sealed class OutboxDeadLetterBrokerPublishException : Exception
    {
        public OutboxDeadLetterBrokerPublishException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
