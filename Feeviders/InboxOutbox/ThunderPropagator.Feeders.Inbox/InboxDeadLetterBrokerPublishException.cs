namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Thrown by <see cref="InboxBrokerDeadLetterHandler"/> when every bounded publish attempt to the broker DLQ fails.</summary>
    public sealed class InboxDeadLetterBrokerPublishException : Exception
    {
        public InboxDeadLetterBrokerPublishException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
