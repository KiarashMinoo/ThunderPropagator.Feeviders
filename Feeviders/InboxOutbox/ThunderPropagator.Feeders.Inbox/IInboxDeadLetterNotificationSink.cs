namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Delivers the one bounded operational notification <see cref="InboxChannelDeadLetterHandler"/>
    /// emits per dead-lettered entry. A caller supplies an implementation bridging to whatever
    /// notification transport makes sense for its own operations (an operational Notifications channel,
    /// a chat webhook, an email, etc.) - this library defines only the recursion-protected calling
    /// contract, not the transport.
    /// </summary>
    public interface IInboxDeadLetterNotificationSink
    {
        /// <summary>
        /// Sends one notification describing <paramref name="context"/>. If this notification is itself
        /// delivered as a message that can re-enter this same dead-letter machinery (e.g. published
        /// through another Inbox/Outbox subscription), the implementation must stamp
        /// <see cref="InboxChannelDeadLetterHandler.RecursionGuardHeader"/> on it - otherwise a
        /// persistently failing notification channel would notify about its own failures forever.
        /// </summary>
        Task NotifyAsync(InboxDeadLetterContext context, CancellationToken cancellationToken);
    }
}
