namespace ThunderPropagator.Feeders.Inbox.Redis
{
    /// <summary>
    /// Builds every Redis key one <see cref="RedisInboxStore"/> instance uses, all sharing one
    /// <see href="https://redis.io/docs/reference/cluster-spec/#hash-tags">hash tag</see> - the
    /// <c>{keyPrefix}</c> segment - so every key a single Lua script touches for one store instance
    /// always maps to the same Redis Cluster slot, regardless of cluster topology. Two stores must use
    /// different <c>keyPrefix</c> values to avoid colliding in the same Redis database - see
    /// <see cref="RedisInboxStoreServiceCollectionExtensions.AddRedisInboxStore"/>, which uses the
    /// store name for this by default.
    /// </summary>
    internal sealed class RedisKeyNamespace(string keyPrefix)
    {
        private readonly string _tag = $"{{{keyPrefix}}}";

        /// <summary>The message hash for entry <paramref name="id"/>.</summary>
        public string Message(Guid id) => $"{MessagePrefix}{id:N}";

        /// <summary>
        /// The constant portion of <see cref="Message"/>, before the id - lets a script that already
        /// has a bare id (e.g. from a sorted set member, as <see cref="RedisInboxStore.PurgeAsync"/>
        /// does) build the full message key itself without a round trip back into C#.
        /// </summary>
        public string MessagePrefix => $"{_tag}:inbox:msg:";

        /// <summary>
        /// String key mapping one (channelKey, partitionKey, messageId) dedup triple to its entry id -
        /// see <see cref="RedisInboxStore.TryClaimAsync"/>.
        /// </summary>
        public string Dedup(Guid channelKey, string? partitionKey, string messageId) =>
            $"{_tag}:inbox:dedup:{System.Text.Json.JsonSerializer.Serialize(new[] { channelKey.ToString(), partitionKey, messageId })}";

        /// <summary>
        /// Sorted set of every non-terminal entry for one channel, scored by whenever it next becomes
        /// retry-eligible (a Processing entry's lease expiry, or a Failed entry's next-retry time) -
        /// backs <see cref="RedisInboxStore.QueryRetryableAsync"/> via <c>ZRANGEBYSCORE</c>.
        /// </summary>
        public string Retryable(Guid channelKey) => $"{_tag}:inbox:retryable:{channelKey:N}";

        /// <summary>
        /// Sorted set of every terminal (Processed/DeadLettered) entry for one channel, scored by the
        /// timestamp it became terminal - backs <see cref="RedisInboxStore.PurgeAsync"/>.
        /// </summary>
        public string Terminal(Guid channelKey) => $"{_tag}:inbox:terminal:{channelKey:N}";
    }
}
