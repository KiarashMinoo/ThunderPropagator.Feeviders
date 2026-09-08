using MongoDB.Bson.Serialization.Attributes;

namespace ThunderPropagator.Feeders.Inbox.MongoDB
{
    /// <summary>The single document <see cref="MongoInboxStore.InitializeAsync"/> reads/writes to track this database's persisted schema version.</summary>
    internal sealed class SchemaVersionDocument
    {
        [BsonId]
        public required string Id { get; set; }

        public int Version { get; set; }
    }
}
