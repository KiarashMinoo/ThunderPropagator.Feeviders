namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Thrown by a Provider's bypass publish (or anything a relay worker calls while relaying a claimed
    /// entry) to signal that a publish failure must not be retried - the relay worker dead-letters the
    /// entry immediately, regardless of <see cref="OutboxOptions.MaxRetryAttempts"/> or attempt count.
    /// Use this for failures no amount of retrying can fix (e.g. the broker permanently rejects the
    /// payload as malformed) - contrast with every other exception, which is treated as transient and
    /// retried per the configured backoff.
    /// </summary>
    public sealed class OutboxNonRetryableException : Exception
    {
        public OutboxNonRetryableException(string message)
            : base(message)
        {
        }

        public OutboxNonRetryableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
