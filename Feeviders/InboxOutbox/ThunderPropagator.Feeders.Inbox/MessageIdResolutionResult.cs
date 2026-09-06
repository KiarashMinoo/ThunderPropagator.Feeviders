namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Outcome of <see cref="IMessageIdResolver.Resolve"/>.</summary>
    public sealed record MessageIdResolutionResult
    {
        /// <summary>The resolved value for <see cref="InboxMessage.MessageId"/>.</summary>
        public required string MessageId { get; init; }

        /// <summary>The strategy that produced <see cref="MessageId"/>.</summary>
        public required InboxMessageIdStrategy Strategy { get; init; }

        /// <summary>
        /// Whether the configured value was missing and <see cref="MessageId"/> is a
        /// <see cref="MessageIdMissingValueBehavior.FallBackToNewGuid"/> fallback rather than a value
        /// actually derived from the message.
        /// </summary>
        public bool IsFallback { get; init; }
    }
}
