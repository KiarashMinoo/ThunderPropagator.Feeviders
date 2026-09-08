namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Forwards a dead-lettered Outbox entry's payload to a broker DLQ - the extension point
    /// <see cref="OutboxBrokerDeadLetterHandler"/> calls. This core project has no direct dependency on
    /// <c>IProvider</c> (that type lives one layer up, in the Providers SharedKernel, which depends on
    /// this project rather than the other way around), so a caller wanting broker DLQ delivery must
    /// supply an implementation - <c>ProviderOutboxDeadLetterBrokerPublisher</c> (in
    /// <c>ThunderPropagator.Providers.DotNet.SharedKernel</c>) is a ready-made one bridging to a real
    /// <c>IProvider</c>.
    /// </summary>
    public interface IOutboxDeadLetterBrokerPublisher
    {
        /// <summary>
        /// Publishes <paramref name="payload"/> (already payload-policy-filtered and optionally
        /// transformed - see <see cref="OutboxBrokerDeadLetterHandler"/>) to the configured DLQ target.
        /// <paramref name="headers"/> carries both the original message headers and the DLQ
        /// idempotency/metadata headers <see cref="OutboxBrokerDeadLetterHandler"/> adds. A thrown
        /// exception is retried by the handler up to its configured bound.
        /// </summary>
        Task PublishAsync(OutboxDeadLetterContext context, byte[] payload, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken);
    }
}
