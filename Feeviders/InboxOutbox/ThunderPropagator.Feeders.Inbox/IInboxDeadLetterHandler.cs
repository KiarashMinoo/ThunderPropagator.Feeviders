namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Optional hook invoked after <see cref="InboxRetryWorker"/> durably marks an entry
    /// <see cref="InboxMessageStatus.DeadLettered"/> - the extension point a future dead-letter pipeline
    /// (routing, alerting, replay tooling) plugs into. Registered per <see cref="InboxRetrySubscription.ChannelKey"/>;
    /// a subscription with no registered handler simply skips this step, so dead-lettering itself never
    /// depends on one existing.
    /// </summary>
    public interface IInboxDeadLetterHandler
    {
        /// <summary>
        /// Notified after <paramref name="message"/> was durably marked DeadLettered.
        /// <paramref name="failureReason"/> is the same sanitized reason persisted on the entry. Errors
        /// thrown here are the caller's responsibility - <see cref="InboxRetryWorker"/> logs and swallows
        /// them rather than treating a notification failure as a reason to reattempt dead-lettering.
        /// </summary>
        ValueTask HandleAsync(InboxMessage message, string failureReason, CancellationToken cancellationToken);
    }
}
