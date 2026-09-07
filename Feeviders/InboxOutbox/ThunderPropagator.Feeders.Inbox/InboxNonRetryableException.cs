namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Thrown by an <see cref="IInboxRetryHandler"/> (or the original receive-path handler) to signal
    /// that a failure must not be retried - <see cref="InboxRetryWorker"/> dead-letters the entry
    /// immediately, regardless of <see cref="InboxOptions.MaxRetryAttempts"/> or attempt count. Use this
    /// for failures no amount of retrying can fix (e.g. the payload fails schema validation, or the
    /// business rule that rejected it will reject it identically next time) - contrast with every other
    /// exception, which is treated as transient and retried per the configured backoff.
    /// </summary>
    public sealed class InboxNonRetryableException : Exception
    {
        public InboxNonRetryableException(string message)
            : base(message)
        {
        }

        public InboxNonRetryableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
