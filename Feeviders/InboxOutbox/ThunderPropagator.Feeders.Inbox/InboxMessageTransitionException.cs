namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Thrown when code asks an <see cref="InboxMessage"/> to move to a status its state machine
    /// does not allow from its current status (see <see cref="InboxMessage.CanTransitionTo"/>).
    /// Prefer <see cref="InboxMessage.TryTransitionTo"/> over the throwing path where an invalid
    /// transition is an expected possibility rather than a programming error.
    /// </summary>
    public sealed class InboxMessageTransitionException : InvalidOperationException
    {
        /// <summary>The status the message was in when the transition was attempted.</summary>
        public InboxMessageStatus From { get; }

        /// <summary>The status that was requested and rejected.</summary>
        public InboxMessageStatus To { get; }

        public InboxMessageTransitionException(InboxMessageStatus from, InboxMessageStatus to)
            : base($"Cannot transition an inbox message from '{from}' to '{to}'.")
        {
            From = from;
            To = to;
        }
    }
}
