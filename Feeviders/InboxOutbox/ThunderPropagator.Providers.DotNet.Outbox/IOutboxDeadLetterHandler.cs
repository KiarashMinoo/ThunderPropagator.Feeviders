namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Optional hook invoked after a relay worker durably marks an entry
    /// <see cref="OutboxMessageStatus.DeadLettered"/> - the extension point a future dead-letter
    /// pipeline (routing, alerting, replay tooling) plugs into. Registered per relay subscription; a
    /// subscription with no registered handler simply skips this step, so dead-lettering itself never
    /// depends on one existing.
    /// </summary>
    public interface IOutboxDeadLetterHandler
    {
        /// <summary>
        /// Notified after <paramref name="message"/> was durably marked DeadLettered.
        /// <paramref name="failureReason"/> is the same sanitized reason persisted on the entry. Errors
        /// thrown here are the caller's responsibility - the relay worker logs and swallows them rather
        /// than treating a notification failure as a reason to reattempt dead-lettering.
        /// </summary>
        ValueTask HandleAsync(OutboxMessage message, string failureReason, CancellationToken cancellationToken);
    }
}
