using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace ThunderPropagator.Feeders.Inbox.MongoDB
{
    /// <summary>
    /// The BSON document shape <see cref="MongoInboxStore"/> persists - not <see cref="InboxMessage"/>
    /// mapped directly (contrast the EF Core backend's direct entity mapping): the driver's POCO
    /// serializer has no equivalent of EF Core's model-only "shadow property", so <see cref="Version"/> -
    /// this document's optimistic-concurrency token, never exposed on the domain type - needs an actual
    /// class member to live on. <see cref="InboxMessageMapper"/> converts both ways, the same role
    /// <c>RedisInboxMessageMapper</c> plays for the Redis backend's own on-the-wire shape.
    /// </summary>
    /// <remarks>
    /// Every identifier (<see cref="Id"/>, <see cref="ChannelKey"/>, <see cref="FeederId"/>) is stored as
    /// a plain string, not a native BSON UUID - side-steps the driver's <c>GuidRepresentation</c>
    /// configuration entirely (a real footgun across driver versions/defaults) and mirrors the Redis
    /// backend's own convention of treating every ID as its hex-string form. Every timestamp is stored as
    /// a plain UTC <see cref="DateTime"/>, not <see cref="DateTimeOffset"/>: the driver's default
    /// <see cref="DateTimeOffset"/> representation is a two-field array/document, not a native BSON date,
    /// which sorts and range-compares incorrectly in a query ($lte/$gte) and cannot back a TTL index at
    /// all. Every field here is always UTC by convention (the <c>*AtUtc</c> naming), so the round trip
    /// through <see cref="DateTime"/> loses nothing.
    /// </remarks>
    internal sealed class InboxMessageDocument
    {
        [BsonId]
        public required string Id { get; set; }

        public required string MessageId { get; set; }

        public required string ChannelKey { get; set; }

        public required string FeederId { get; set; }

        public string? PartitionKey { get; set; }

        public int SchemaVersion { get; set; }

        public required string PayloadContentType { get; set; }

        public required byte[] Payload { get; set; }

        public Dictionary<string, string> Headers { get; set; } = [];

        [BsonRepresentation(BsonType.String)]
        public InboxMessageStatus Status { get; set; }

        public int AttemptCount { get; set; }

        public DateTime ReceivedAtUtc { get; set; }

        public DateTime? ProcessingStartedAtUtc { get; set; }

        public DateTime? ProcessedAtUtc { get; set; }

        public DateTime? DeadLetteredAtUtc { get; set; }

        public DateTime? NextRetryAtUtc { get; set; }

        public string? LeaseOwner { get; set; }

        public DateTime? LeaseExpiresAtUtc { get; set; }

        public string? FailureReason { get; set; }

        /// <summary>
        /// Set only once <see cref="Status"/> reaches a terminal state (<see cref="InboxMessageStatus.Processed"/>/
        /// <see cref="InboxMessageStatus.DeadLettered"/>) - the field the TTL index in
        /// <see cref="MongoInboxStore.EnsureIndexesAsync"/> targets. A non-terminal document always has
        /// this <see langword="null"/> (a missing/null value never matches a TTL index), so it can never
        /// expire out from under an in-flight claim or an unretried failure.
        /// </summary>
        public DateTime? TerminalAtUtc { get; set; }

        /// <summary>
        /// Optimistic-concurrency token - see the class remarks. Every reclaim/lease-scoped write is a
        /// single <c>findOneAndUpdate</c> filtered on the value this document had when read, incrementing
        /// it as part of the same atomic operation.
        /// </summary>
        public long Version { get; set; }
    }
}
