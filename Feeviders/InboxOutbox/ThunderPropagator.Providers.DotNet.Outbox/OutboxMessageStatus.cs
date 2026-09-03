namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Lifecycle states for an <see cref="OutboxMessage"/>. See
    /// <see cref="OutboxMessage.CanTransitionTo"/> for the allowed transition graph.
    /// </summary>
    public enum OutboxMessageStatus
    {
        /// <summary>Durably enqueued (in the same local transaction as business state, where a unit of work is enlisted) but not yet claimed for publishing.</summary>
        Pending,

        /// <summary>A relay worker holds an active publishing lease on the message.</summary>
        Publishing,

        /// <summary>The broker adapter's acknowledgement condition was met. Terminal.</summary>
        Published,

        /// <summary>
        /// Publishing failed. Not terminal - eligible for a further retry claim (back to
        /// <see cref="Publishing"/>) until retry policy is exhausted, at which point it moves to
        /// <see cref="DeadLettered"/>.
        /// </summary>
        Failed,

        /// <summary>Retry policy was exhausted, or the failure was flagged non-retryable. Terminal.</summary>
        DeadLettered
    }
}
