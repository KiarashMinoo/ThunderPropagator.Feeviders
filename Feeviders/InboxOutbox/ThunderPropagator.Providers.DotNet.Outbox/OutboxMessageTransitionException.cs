namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Thrown when code asks an <see cref="OutboxMessage"/> to move to a status its state machine
    /// does not allow from its current status (see <see cref="OutboxMessage.CanTransitionTo"/>).
    /// Prefer <see cref="OutboxMessage.TryTransitionTo"/> over the throwing path where an invalid
    /// transition is an expected possibility rather than a programming error.
    /// </summary>
    public sealed class OutboxMessageTransitionException : InvalidOperationException
    {
        /// <summary>The status the message was in when the transition was attempted.</summary>
        public OutboxMessageStatus From { get; }

        /// <summary>The status that was requested and rejected.</summary>
        public OutboxMessageStatus To { get; }

        public OutboxMessageTransitionException(OutboxMessageStatus from, OutboxMessageStatus to)
            : base($"Cannot transition an outbox message from '{from}' to '{to}'.")
        {
            From = from;
            To = to;
        }
    }
}
