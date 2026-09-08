using MongoDB.Bson.Serialization.Attributes;

namespace ThunderPropagator.Providers.DotNet.Outbox.MongoDB
{
    /// <summary>
    /// One partition's ordering-sequence counter. <c>_id</c> is the partition's own key (a sentinel
    /// string for the <see langword="null"/> partition - see <see cref="MongoOutboxStore.NullPartitionId"/>),
    /// so allocating the next sequence number for a partition is a single, natively atomic
    /// <c>findOneAndUpdate</c> with <c>$inc</c> and <c>upsert: true</c> - no read-decide-write/optimistic-
    /// concurrency loop needed the way the EF Core backend's own sequence counter requires, since MongoDB's
    /// <c>$inc</c> is itself the atomic primitive here.
    /// </summary>
    internal sealed class OutboxSequenceDocument
    {
        [BsonId]
        public required string Id { get; set; }

        public long NextValue { get; set; }
    }
}
