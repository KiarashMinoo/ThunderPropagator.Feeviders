using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    public interface IProvider : IDisposable
    {
        /// <summary>
        /// Convenience overload of <see cref="PublishDirectAsync(byte[], IReadOnlyDictionary{string, string}?, CancellationToken)"/>
        /// with no headers.
        /// </summary>
        Task PublishDirectAsync(byte[] bytes, CancellationToken cancellationToken = default) =>
            PublishDirectAsync(bytes, headers: null, cancellationToken);

        /// <summary>
        /// Publishes <paramref name="bytes"/> directly, bypassing the Outbox enable/enqueue decision
        /// entirely - the bypass path a relay worker (reclaiming an already-durable
        /// <c>OutboxMessage.Payload</c>) must use, so a relayed publish can never recurse back into
        /// enqueuing the same message again. <paramref name="headers"/> carries whatever accompanied the
        /// original message (e.g. <c>OutboxMessage.Headers</c>, including injected trace context) - a
        /// Provider with no way to attach transport-level headers to a raw-bytes publish is free to
        /// ignore it. A default interface implementation (throwing) keeps this source-compatible for any
        /// existing <see cref="IProvider"/> implementer that predates this member; <c>AbstractProvider</c>
        /// overrides it with a real implementation.
        /// </summary>
        Task PublishDirectAsync(byte[] bytes, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException($"{GetType().Name} does not support {nameof(PublishDirectAsync)}.");
    }

    public interface IProvider<in TFeederMessage> : IProvider
        where TFeederMessage : FeederMessage
    {
        Task ExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default);
    }
}