namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Forwards a dead-lettered Inbox entry's payload to a broker DLQ - the extension point
    /// <see cref="InboxBrokerDeadLetterHandler"/> calls. Feeders is receive-only by design (it has no
    /// publish-side dependency), so a caller wanting broker DLQ delivery must supply an implementation
    /// bridging to whatever outbound mechanism its own composition root has access to - typically a
    /// Provider's direct-publish path from the Outbox side of the same application.
    /// </summary>
    public interface IInboxDeadLetterBrokerPublisher
    {
        /// <summary>
        /// Publishes <paramref name="payload"/> (already payload-policy-filtered and optionally
        /// transformed - see <see cref="InboxBrokerDeadLetterHandler"/>) to the configured DLQ target.
        /// <paramref name="headers"/> carries both the original message headers and the DLQ
        /// idempotency/metadata headers <see cref="InboxBrokerDeadLetterHandler"/> adds. A thrown
        /// exception is retried by the handler up to its configured bound.
        /// </summary>
        Task PublishAsync(InboxDeadLetterContext context, byte[] payload, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken);
    }
}
