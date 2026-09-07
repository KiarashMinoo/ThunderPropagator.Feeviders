namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Reprocesses one Inbox entry <see cref="InboxRetryWorker"/> has reclaimed - the retry-path
    /// counterpart of whatever handler a Feevider's own receive path (e.g.
    /// <c>InboxReceiveCoordinator</c>) invoked live. Registered per <see cref="InboxRetrySubscription.ChannelKey"/>;
    /// <see cref="InboxRetryWorker"/> never deserializes a payload itself, so an implementation is
    /// responsible for interpreting <see cref="InboxMessage.Payload"/> according to
    /// <see cref="InboxMessage.PayloadContentType"/> and <see cref="InboxMessage.SchemaVersion"/> - and,
    /// where a channel's message shape has changed across versions, routing to whichever handler logic
    /// matches the stored <see cref="InboxMessage.SchemaVersion"/>.
    /// </summary>
    public interface IInboxRetryHandler
    {
        /// <summary>
        /// Reprocesses <paramref name="message"/>. Throw <see cref="InboxNonRetryableException"/> for a
        /// failure that must dead-letter immediately regardless of remaining attempts; any other
        /// exception is treated as transient and retried per the subscription's backoff configuration.
        /// </summary>
        ValueTask HandleAsync(InboxMessage message, CancellationToken cancellationToken);
    }
}
