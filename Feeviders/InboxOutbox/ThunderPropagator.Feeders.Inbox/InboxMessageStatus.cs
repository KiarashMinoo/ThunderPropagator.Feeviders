namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Lifecycle states for an <see cref="InboxMessage"/>. See
    /// <see cref="InboxMessage.CanTransitionTo"/> for the allowed transition graph and
    /// <see cref="InboxMessage.TryTransitionTo"/>/<see cref="InboxMessage.Replay"/> for how a
    /// message actually moves between states.
    /// </summary>
    public enum InboxMessageStatus
    {
        /// <summary>Durably recorded but not yet claimed by any worker.</summary>
        Received,

        /// <summary>A worker holds an active processing lease on the message.</summary>
        Processing,

        /// <summary>The message was handled successfully. Terminal.</summary>
        Processed,

        /// <summary>
        /// Processing failed. Not terminal - eligible for a further retry claim (back to
        /// <see cref="Processing"/>) until retry policy is exhausted, at which point it moves to
        /// <see cref="DeadLettered"/>.
        /// </summary>
        Failed,

        /// <summary>Retry policy was exhausted, or the failure was flagged non-retryable. Terminal.</summary>
        DeadLettered
    }
}
