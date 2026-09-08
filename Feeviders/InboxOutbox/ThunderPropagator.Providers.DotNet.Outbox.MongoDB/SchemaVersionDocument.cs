using MongoDB.Bson.Serialization.Attributes;

namespace ThunderPropagator.Providers.DotNet.Outbox.MongoDB
{
    /// <summary>The single document <see cref="MongoOutboxStore.InitializeAsync"/> reads/writes to track this database's persisted schema version.</summary>
    internal sealed class SchemaVersionDocument
    {
        [BsonId]
        public required string Id { get; set; }

        public int Version { get; set; }
    }
}
