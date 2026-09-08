using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    /// <summary>
    /// Ready-made <see cref="IOutboxDeadLetterBrokerPublisher"/> bridging
    /// <see cref="OutboxBrokerDeadLetterHandler"/> to a real <see cref="IProvider"/> via
    /// <see cref="IProvider.PublishDirectAsync(byte[], IReadOnlyDictionary{string, string}?, CancellationToken)"/> -
    /// the same bypass-Outbox publish path a relay worker uses to republish an already-durable message,
    /// so forwarding a dead letter to a DLQ can never recurse back into re-enqueuing the message it is
    /// forwarding. <paramref name="provider"/> should resolve to whichever Provider instance is
    /// configured as the DLQ target, not the Provider the failed message originally targeted.
    /// </summary>
    public sealed class ProviderOutboxDeadLetterBrokerPublisher(IProvider provider) : IOutboxDeadLetterBrokerPublisher
    {
        public Task PublishAsync(OutboxDeadLetterContext context, byte[] payload, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken) =>
            provider.PublishDirectAsync(payload, headers, cancellationToken);
    }
}
