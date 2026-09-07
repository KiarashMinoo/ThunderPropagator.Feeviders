namespace ThunderPropagator.Providers.DotNet.Outbox.Redis
{
    /// <summary>
    /// Builds every Redis key one <see cref="RedisOutboxStore"/> instance uses, all sharing one
    /// <see href="https://redis.io/docs/reference/cluster-spec/#hash-tags">hash tag</see> - the
    /// <c>{keyPrefix}</c> segment - so every key a single Lua script touches for one store instance
    /// always maps to the same Redis Cluster slot, regardless of cluster topology. Two stores must use
    /// different <c>keyPrefix</c> values to avoid colliding in the same Redis database - see
    /// <see cref="RedisOutboxStoreServiceCollectionExtensions.AddRedisOutboxStore"/>, which uses the
    /// store name for this by default.
    /// </summary>
    /// <remarks>
    /// A null <see cref="OutboxMessage.PartitionKey"/> is tracked under a fixed sentinel slot -
    /// <see cref="PartitionSlot"/> - that can never collide with a real (non-empty, caller-supplied)
    /// partition key.
    /// </remarks>
    internal sealed class RedisKeyNamespace(string keyPrefix)
    {
        private const string NullPartitionSentinel = "~";

        private readonly string _tag = $"{{{keyPrefix}}}";

        /// <summary>The message hash for entry <paramref name="id"/>.</summary>
        public string Message(Guid id) => $"{MessagePrefix}{id:N}";

        /// <summary>
        /// The constant portion of <see cref="Message"/>, before the id - lets a script that already
        /// has a bare id (e.g. from a sorted set member) build the full message key itself without a
        /// round trip back into C#.
        /// </summary>
        public string MessagePrefix => $"{_tag}:outbox:msg:";

        /// <summary>Maps a nullable <see cref="OutboxMessage.PartitionKey"/> to its fixed, non-null slot name.</summary>
        public static string PartitionSlot(string? partitionKey) => partitionKey ?? NullPartitionSentinel;

        /// <summary>Atomic ordering-sequence counter for one partition - <c>INCR</c>'d once per <see cref="RedisOutboxStore.EnqueueAsync"/> call.</summary>
        public string OrderingSequence(string partitionSlot) => $"{_tag}:outbox:seq:{partitionSlot}";

        /// <summary>
        /// Sorted set of every non-terminal (Pending/Publishing/Failed) entry in one partition, scored
        /// by <see cref="OutboxMessage.OrderingSequence"/> - backs <see cref="RedisOutboxStore.ClaimBatchAsync"/>,
        /// <see cref="RedisOutboxStore.GetDepthAsync"/>, and <see cref="RedisOutboxStore.GetOldestPendingAgeAsync"/>.
        /// </summary>
        public string Active(string partitionSlot) => $"{_tag}:outbox:active:{partitionSlot}";

        /// <summary>
        /// Set of every partition slot with a non-empty <see cref="Active"/> set right now - lets
        /// <see cref="RedisOutboxStore.GetClaimablePartitionKeysAsync"/> and aggregate (<c>null</c>-scoped)
        /// <see cref="RedisOutboxStore.GetDepthAsync"/>/<see cref="RedisOutboxStore.GetOldestPendingAgeAsync"/>
        /// enumerate partitions without an unbounded <c>SCAN</c>.
        /// </summary>
        public string KnownPartitions => $"{_tag}:outbox:partitions";

        /// <summary>Sorted set of every terminal (Published/DeadLettered) entry, scored by when it became terminal - backs <see cref="RedisOutboxStore.PurgeAsync"/>.</summary>
        public string Terminal => $"{_tag}:outbox:terminal";
    }
}
