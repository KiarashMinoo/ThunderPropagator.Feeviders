namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Thrown by <see cref="IMessageIdResolver.Resolve"/> when the configured
    /// <see cref="InboxMessageIdStrategy"/> cannot produce a value - a missing header/payload/feeder
    /// field and <see cref="MessageIdResolverOptions.MissingValueBehavior"/> is
    /// <see cref="MessageIdMissingValueBehavior.Throw"/>, or the resolved ID exceeds
    /// <see cref="InboxMessageLimits.MaxMessageIdLength"/>.
    /// </summary>
    public sealed class MessageIdResolutionException : Exception
    {
        /// <summary>The strategy that failed to resolve a value.</summary>
        public InboxMessageIdStrategy Strategy { get; }

        public MessageIdResolutionException(InboxMessageIdStrategy strategy, string message)
            : base(message)
        {
            Strategy = strategy;
        }
    }
}
