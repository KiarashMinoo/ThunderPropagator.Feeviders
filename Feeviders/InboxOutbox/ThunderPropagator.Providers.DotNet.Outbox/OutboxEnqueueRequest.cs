namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Everything an <see cref="IOutboxStore"/> needs to enqueue one outbound message. See
    /// <see cref="IOutboxStore.EnqueueAsync"/>.
    /// </summary>
    public sealed record OutboxEnqueueRequest
    {
        /// <summary>Stable, caller-supplied-or-generated idempotency/message identifier.</summary>
        public required string MessageId { get; init; }

        /// <summary>The publishing provider/destination this message targets.</summary>
        public required string ProviderKey { get; init; }

        /// <summary>The ordering partition this message belongs to.</summary>
        public string? PartitionKey { get; init; }

        /// <summary>Schema version of <see cref="Payload"/>.</summary>
        public required int SchemaVersion { get; init; }

        /// <summary>Content type of <see cref="Payload"/>.</summary>
        public required string PayloadContentType { get; init; }

        /// <summary>The raw, serialized message payload.</summary>
        public required byte[] Payload { get; init; }

        /// <summary>Optional bounded header set.</summary>
        public IReadOnlyDictionary<string, string>? Headers { get; init; }
    }
}
