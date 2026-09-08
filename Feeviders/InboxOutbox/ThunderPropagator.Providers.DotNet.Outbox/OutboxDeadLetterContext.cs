namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Everything an <see cref="IOutboxDeadLetterHandler"/> needs about one entry that just reached
    /// <see cref="OutboxMessageStatus.DeadLettered"/> - built once (see <see cref="OutboxDeadLetterContextFactory"/>)
    /// and handed to every handler in the pipeline, so handlers never need their own <see cref="IOutboxStore"/>
    /// reference just to look up what happened.
    /// </summary>
    public sealed record OutboxDeadLetterContext
    {
        /// <summary>The store-assigned identity of the dead-lettered entry - <see cref="OutboxMessage.Id"/>.</summary>
        public required Guid Id { get; init; }

        /// <summary>The caller/business message identifier.</summary>
        public required string MessageId { get; init; }

        /// <summary>The publishing provider/destination this message targeted.</summary>
        public required string ProviderKey { get; init; }

        /// <summary>The ordering partition this message belonged to.</summary>
        public string? PartitionKey { get; init; }

        /// <summary>Where this message was headed - the provider/destination it targeted.</summary>
        public string Target => ProviderKey;

        /// <summary>Number of publish attempts made before this entry was dead-lettered.</summary>
        public required int Attempts { get; init; }

        /// <summary>When the message was enqueued.</summary>
        public required DateTimeOffset CreatedAtUtc { get; init; }

        /// <summary>When the message reached <see cref="OutboxMessageStatus.DeadLettered"/>.</summary>
        public required DateTimeOffset DeadLetteredAtUtc { get; init; }

        /// <summary>Why this entry was dead-lettered - see <see cref="OutboxFailureClassifier"/>.</summary>
        public required OutboxDeadLetterFailureCategory FailureCategory { get; init; }

        /// <summary>The same sanitized (type-name-only, never the raw exception message) reason persisted on the entry.</summary>
        public required string FailureReason { get; init; }

        /// <summary>
        /// The <see cref="System.Diagnostics.Activity.Id"/> in effect when this entry was dead-lettered,
        /// if any - correlates a dead-letter record back to the distributed trace that produced it.
        /// </summary>
        public string? TraceId { get; init; }

        /// <summary>
        /// The original payload, or <see langword="null"/> when <see cref="OutboxDeadLetterPayloadPolicy"/>
        /// excluded it - see <see cref="OutboxDeadLetterContextFactory"/>'s remarks.
        /// </summary>
        public byte[]? Payload { get; init; }

        /// <summary>Content type describing <see cref="Payload"/>, when present.</summary>
        public string? PayloadContentType { get; init; }

        /// <summary>The original bounded header set.</summary>
        public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();
    }
}
