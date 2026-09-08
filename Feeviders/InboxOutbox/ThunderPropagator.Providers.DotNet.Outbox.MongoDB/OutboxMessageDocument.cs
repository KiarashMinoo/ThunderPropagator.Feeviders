using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace ThunderPropagator.Providers.DotNet.Outbox.MongoDB
{
    /// <summary>
    /// The BSON document shape <see cref="MongoOutboxStore"/> persists - see
    /// <c>ThunderPropagator.Feeders.Inbox.MongoDB.InboxMessageDocument</c>'s remarks for why this is a
    /// dedicated document type rather than <see cref="OutboxMessage"/> mapped directly, and why every
    /// identifier/timestamp is a plain string/<see cref="DateTime"/>. <see cref="OutboxMessageMapper"/>
    /// converts both ways.
    /// </summary>
    internal sealed class OutboxMessageDocument
    {
        [BsonId]
        public required string Id { get; set; }

        public required string MessageId { get; set; }

        public required string ProviderKey { get; set; }

        public string? PartitionKey { get; set; }

        public long OrderingSequence { get; set; }

        public int SchemaVersion { get; set; }

        public required string PayloadContentType { get; set; }

        public required byte[] Payload { get; set; }

        public Dictionary<string, string> Headers { get; set; } = [];

        [BsonRepresentation(BsonType.String)]
        public OutboxMessageStatus Status { get; set; }

        public int Attempts { get; set; }

        public DateTime CreatedAtUtc { get; set; }

        public DateTime? PublishingStartedAtUtc { get; set; }

        public DateTime? PublishedAtUtc { get; set; }

        public DateTime? DeadLetteredAtUtc { get; set; }

        public DateTime? NextRetryAtUtc { get; set; }

        public string? LeaseOwner { get; set; }

        public DateTime? LeaseExpiresAtUtc { get; set; }

        public string? FailureReason { get; set; }

        /// <summary>
        /// Set only once <see cref="Status"/> reaches a terminal state (<see cref="OutboxMessageStatus.Published"/>/
        /// <see cref="OutboxMessageStatus.DeadLettered"/>) - the field the TTL index in
        /// <see cref="MongoOutboxStore.EnsureIndexesAsync"/> targets. See
        /// <c>ThunderPropagator.Feeders.Inbox.MongoDB.InboxMessageDocument.TerminalAtUtc</c>'s remarks.
        /// </summary>
        public DateTime? TerminalAtUtc { get; set; }

        /// <summary>Optimistic-concurrency token - see <see cref="MongoOutboxStore"/>'s remarks.</summary>
        public long Version { get; set; }
    }
}
