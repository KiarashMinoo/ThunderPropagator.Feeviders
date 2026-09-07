using System.Text.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace ThunderPropagator.Feeders.Inbox.Redis
{
    /// <summary>
    /// <see cref="IInboxStore"/> backed by <see href="https://redis.io/">Redis</see> via
    /// StackExchange.Redis. Durable across restarts as long as the Redis deployment itself persists
    /// (RDB/AOF) - unlike <c>InMemoryInboxStore</c>, an entry survives this process exiting.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Atomicity.</b> Every field-level decision (whether a claim is allowed, what the new state
    /// looks like) is computed in C# using <see cref="InboxMessage"/>'s own transition rules - exactly
    /// the same rules <c>InMemoryInboxStore</c> uses - so the two backends can never silently diverge
    /// on what is/isn't a valid transition. Redis's role is purely to make the read-decide-write
    /// sequence atomic: every mutation is either a single Lua script (an all-or-nothing compare-and-swap
    /// keyed on either the entry's monotonic <c>Version</c> field, for a fresh claim/reclaim where there
    /// is no prior owner identity to check, or its current <c>LeaseOwner</c>, for every lease-scoped
    /// operation - <see cref="CompleteAsync"/>, <see cref="FailAsync"/>, <see cref="DeadLetterAsync"/>,
    /// <see cref="RenewLeaseAsync"/>) or a single atomic create-if-absent script for a brand-new dedup
    /// key. A version-CAS conflict retries the whole read-decide-write cycle (bounded by
    /// <see cref="MaxCasAttempts"/>) rather than failing outright, since the entry's state - and
    /// therefore the correct outcome - may have changed while retrying.
    /// </para>
    /// <para>
    /// <b>Keys.</b> Every key this instance touches shares one Redis Cluster hash tag (the
    /// <c>keyPrefix</c> constructor argument, wrapped in <c>{}</c>) so a multi-key Lua script never
    /// spans more than one cluster slot - see <see cref="RedisKeyNamespace"/>. Two stores sharing one
    /// Redis database/cluster MUST use different <c>keyPrefix</c>es or their keyspaces will
    /// collide.
    /// </para>
    /// <para>
    /// <b>Retention.</b> <see cref="PurgeAsync"/> is the primary retention mechanism, exactly like
    /// <c>InMemoryInboxStore</c>. Optionally, <c>terminalEntryTtl</c> additionally applies a
    /// native Redis <c>EXPIRE</c> to an entry's hash key the moment it becomes terminal (Processed or
    /// DeadLettered) - a passive safety net if <see cref="PurgeAsync"/> is never called - but never to
    /// a non-terminal entry: a Received/Processing/Failed entry's key never has a TTL set on it, so it
    /// can never expire out from under an in-flight claim or an unretried failure, regardless of how
    /// long that takes.
    /// </para>
    /// <para>
    /// <b>Reconnects/timeouts/cluster.</b> This store does not itself manage reconnection - it uses
    /// whatever <see cref="IConnectionMultiplexer"/> the caller supplies, and StackExchange.Redis's own
    /// automatic-reconnect behavior applies exactly as the caller configured it (see
    /// <c>ConfigurationOptions.AbortOnConnectFail</c>/<c>ReconnectRetryPolicy</c>). A
    /// <see cref="RedisConnectionException"/>/<see cref="RedisTimeoutException"/> during any operation
    /// propagates to the caller uncaught - a relay/retry worker's own existing exception handling (see
    /// <c>OutboxRelayWorker</c>/<c>InboxRetryWorker</c>) already treats an unexpected exception as
    /// transient and retries with backoff, which is the correct response to a Redis outage. Every script
    /// here uses <see cref="StackExchange.Redis.LuaScript"/>, whose evaluate call transparently falls
    /// back from <c>EVALSHA</c> to a full <c>EVAL</c> on a cache miss (e.g. after a failover to a replica
    /// that never had the script loaded) - no manual script-cache management needed.
    /// </para>
    /// </remarks>
    public sealed class RedisInboxStore : IInboxStore, IHealthCheck
    {
        /// <summary>
        /// Upper bound on how many times a version-CAS conflict during <see cref="TryClaimAsync"/>
        /// retries the read-decide-write cycle before giving up. Exists only to bound pathological
        /// contention (many workers claiming the exact same entry at once) - ordinary contention
        /// resolves in one or two attempts.
        /// </summary>
        private const int MaxCasAttempts = 20;

        // StackExchange.Redis's LuaScript named-parameter binding only supports scalar (RedisKey/RedisValue)
        // parameters, not arrays - every variable-length field/value list below travels as one JSON-encoded
        // scalar instead, decoded in Lua via cjson.decode (built into Redis's Lua runtime). @payload is the
        // one exception: it is raw, arbitrary bytes, never valid to embed as a JSON string, so it always
        // travels as its own separate, natively binary-safe scalar - see RedisInboxMessageMapper's remarks.
        private static readonly LuaScript CreateIfAbsentScript = LuaScript.Prepare(
            """
            if redis.call('EXISTS', @dedupKey) == 1 then
              return 0
            end
            redis.call('SET', @dedupKey, @id)
            redis.call('ZADD', @retryableKey, @retryableScore, @id)
            redis.call('HSET', @messageKey, 'Payload', @payload)
            local fields = cjson.decode(@fieldsJson)
            for i = 1, #fields, 2 do
              redis.call('HSET', @messageKey, fields[i], fields[i + 1])
            end
            return 1
            """);

        private static readonly LuaScript WriteScript = LuaScript.Prepare(
            """
            local current
            if @casMode == 'version' then
              current = redis.call('HGET', @messageKey, 'Version')
            else
              current = redis.call('HGET', @messageKey, 'LeaseOwner')
            end
            if current ~= @casValue then
              return 0
            end
            if @retryableOp == 'remove' then
              redis.call('ZREM', @retryableKey, @id)
            elseif @retryableOp ~= 'none' then
              redis.call('ZADD', @retryableKey, @retryableOp, @id)
            end
            if @terminalOp ~= 'none' then
              redis.call('ZADD', @terminalKey, @terminalOp, @id)
            end
            local setFields = cjson.decode(@setFieldsJson)
            for i = 1, #setFields, 2 do
              redis.call('HSET', @messageKey, setFields[i], setFields[i + 1])
            end
            local deleteFields = cjson.decode(@deleteFieldsJson)
            for i = 1, #deleteFields do
              redis.call('HDEL', @messageKey, deleteFields[i])
            end
            if @ttlSeconds ~= '' then
              redis.call('EXPIRE', @messageKey, @ttlSeconds)
            end
            return 1
            """);

        private static readonly LuaScript PurgeScript = LuaScript.Prepare(
            """
            local ids = cjson.decode(@idsJson)
            for i = 1, #ids do
              local messageKey = @keyPrefix .. ids[i]
              local dedupKey = redis.call('HGET', messageKey, 'DedupKey')
              if dedupKey then
                redis.call('DEL', dedupKey)
              end
              redis.call('DEL', messageKey)
              redis.call('ZREM', @terminalKey, ids[i])
            end
            return #ids
            """);

        private readonly IConnectionMultiplexer _connectionMultiplexer;
        private readonly RedisKeyNamespace _keys;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan? _terminalEntryTtl;

        /// <param name="connectionMultiplexer">Owned by the caller - never disposed by this store. See the class remarks for reconnect/failover behavior.</param>
        /// <param name="keyPrefix">Namespaces every key this instance uses - see <see cref="RedisKeyNamespace"/>. Must be unique per logical store sharing a Redis database.</param>
        /// <param name="timeProvider">Clock used for timestamps and lease/retry expiry. Defaults to <see cref="TimeProvider.System"/>.</param>
        /// <param name="terminalEntryTtl">Passive-safety-net TTL applied to an entry's key only once it becomes terminal - see the class remarks. <see langword="null"/> (default) disables it, relying solely on explicit <see cref="PurgeAsync"/> calls.</param>
        public RedisInboxStore(IConnectionMultiplexer connectionMultiplexer, string keyPrefix, TimeProvider? timeProvider = null, TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(connectionMultiplexer);
            ArgumentException.ThrowIfNullOrWhiteSpace(keyPrefix);

            _connectionMultiplexer = connectionMultiplexer;
            _keys = new RedisKeyNamespace(keyPrefix);
            _timeProvider = timeProvider ?? TimeProvider.System;
            _terminalEntryTtl = terminalEntryTtl;
        }

        private IDatabase Database => _connectionMultiplexer.GetDatabase();

        /// <inheritdoc/>
        public async Task<InboxClaimResult> TryClaimAsync(InboxClaimRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            var db = Database;
            var dedupKey = _keys.Dedup(request.ChannelKey, request.PartitionKey, request.MessageId);

            if (await TryCreateAsync(db, request, dedupKey).ConfigureAwait(false) is { } created)
                return InboxClaimResult.Claimed(created);

            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var idValue = await db.StringGetAsync(dedupKey).ConfigureAwait(false);
                if (idValue.IsNull)
                {
                    // The entry that won the create race was purged before we could read it back -
                    // vanishingly rare, but the correct response is simply to try creating it again.
                    if (await TryCreateAsync(db, request, dedupKey).ConfigureAwait(false) is { } recreated)
                        return InboxClaimResult.Claimed(recreated);
                    continue;
                }

                var id = Guid.ParseExact((string)idValue!, "N");
                var hashEntries = await db.HashGetAllAsync(_keys.Message(id)).ConfigureAwait(false);
                if (hashEntries.Length == 0)
                    continue; // Purged between the dedup read and the hash read - retry from the top.

                var existing = RedisInboxMessageMapper.FromHashEntries(hashEntries);
                var version = RedisInboxMessageMapper.ReadVersion(hashEntries);
                var now = _timeProvider.GetUtcNow();
                var retryEligible = existing.NextRetryAtUtc is null || existing.NextRetryAtUtc <= now;
                var leaseExpired = existing.LeaseExpiresAtUtc is null || existing.LeaseExpiresAtUtc <= now;

                switch (existing.Status)
                {
                    case InboxMessageStatus.Processed:
                        return InboxClaimResult.AlreadyProcessed(existing);
                    case InboxMessageStatus.DeadLettered:
                        return InboxClaimResult.DeadLettered(existing);
                    case InboxMessageStatus.Processing when !leaseExpired:
                        return InboxClaimResult.ClaimedByAnotherOwner(existing);
                    case InboxMessageStatus.Failed when !retryEligible:
                        return InboxClaimResult.ClaimedByAnotherOwner(existing);
                }

                // A lapsed Processing lease must first release back through Failed (Processing isn't a
                // valid source for Processing in InboxMessage's transition table) before it can be reclaimed.
                var source = existing.Status == InboxMessageStatus.Processing
                    ? existing.TryTransitionTo(InboxMessageStatus.Failed, _timeProvider)
                    : existing;
                var reclaimed = source.TryTransitionTo(
                    InboxMessageStatus.Processing, _timeProvider,
                    leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

                var applied = await WriteAsync(db, id, reclaimed, version + 1, casMode: "version", casValue: version.ToString()).ConfigureAwait(false);
                if (applied)
                    return InboxClaimResult.Claimed(reclaimed);
            }

            throw new InvalidOperationException(
                $"Exceeded {MaxCasAttempts} attempts contending for an Inbox claim on message '{request.MessageId}' - this indicates pathological concurrency, not a normal outcome.");
        }

        private async Task<InboxMessage?> TryCreateAsync(IDatabase db, InboxClaimRequest request, string dedupKey)
        {
            var now = _timeProvider.GetUtcNow();
            var id = Guid.NewGuid();
            var received = InboxMessage.CreateReceived(
                id, request.MessageId, request.ChannelKey, request.FeederId, request.SchemaVersion,
                request.PayloadContentType, request.Payload, request.Headers, request.PartitionKey, _timeProvider);
            var claimed = received.TryTransitionTo(
                InboxMessageStatus.Processing, _timeProvider,
                leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

            var fieldsJson = RedisInboxMessageMapper.ToFullFieldListJson(claimed, version: 1, dedupKey);
            var result = (int)await db.ScriptEvaluateAsync(CreateIfAbsentScript, new
            {
                dedupKey = (RedisKey)dedupKey,
                messageKey = (RedisKey)_keys.Message(id),
                retryableKey = (RedisKey)_keys.Retryable(request.ChannelKey),
                id = id.ToString("N"),
                retryableScore = RedisInboxMessageMapper.RetryableScore(claimed)!.Value,
                payload = (RedisValue)claimed.Payload,
                fieldsJson,
            }).ConfigureAwait(false);

            return result == 1 ? claimed : null;
        }

        /// <summary>Applies a CAS-guarded write for an already-existing entry, including its retry-tracking/terminal zset side effects and (for a terminal write) the passive TTL.</summary>
        private async Task<bool> WriteAsync(IDatabase db, Guid id, InboxMessage updated, long newVersion, string casMode, string casValue)
        {
            var (setFieldsJson, deleteFieldsJson) = RedisInboxMessageMapper.ToChangedFieldListJson(updated, newVersion);
            var retryableScore = RedisInboxMessageMapper.RetryableScore(updated);
            var isTerminal = updated.Status is InboxMessageStatus.Processed or InboxMessageStatus.DeadLettered;
            var terminalTimestamp = updated.Status switch
            {
                InboxMessageStatus.Processed => updated.ProcessedAtUtc,
                InboxMessageStatus.DeadLettered => updated.DeadLetteredAtUtc,
                _ => null,
            };

            var result = (int)await db.ScriptEvaluateAsync(WriteScript, new
            {
                messageKey = (RedisKey)_keys.Message(id),
                retryableKey = (RedisKey)_keys.Retryable(updated.ChannelKey),
                terminalKey = (RedisKey)_keys.Terminal(updated.ChannelKey),
                id = id.ToString("N"),
                casMode,
                casValue,
                retryableOp = retryableScore is { } score ? score.ToString() : "remove",
                terminalOp = isTerminal ? terminalTimestamp!.Value.ToUnixTimeMilliseconds().ToString() : "none",
                setFieldsJson,
                deleteFieldsJson,
                ttlSeconds = isTerminal && _terminalEntryTtl is { } ttl ? ((long)ttl.TotalSeconds).ToString() : "",
            }).ConfigureAwait(false);

            return result == 1;
        }

        /// <inheritdoc/>
        public async Task<InboxMessage?> GetAsync(string messageId, Guid channelKey, string? partitionKey, CancellationToken cancellationToken = default)
        {
            var db = Database;
            var idValue = await db.StringGetAsync(_keys.Dedup(channelKey, partitionKey, messageId)).ConfigureAwait(false);
            if (idValue.IsNull)
                return null;

            var entries = await db.HashGetAllAsync(_keys.Message(Guid.ParseExact((string)idValue!, "N"))).ConfigureAwait(false);
            return entries.Length == 0 ? null : RedisInboxMessageMapper.FromHashEntries(entries);
        }

        /// <inheritdoc/>
        public Task<InboxMessage?> CompleteAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Processed, _timeProvider));

        /// <inheritdoc/>
        public Task<InboxMessage?> FailAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Failed, _timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason));

        /// <inheritdoc/>
        public Task<InboxMessage?> DeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.DeadLettered, _timeProvider, failureReason: failureReason));

        /// <inheritdoc/>
        public Task<InboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.RenewLease(_timeProvider, leaseOwner, leaseExtension));

        private async Task<InboxMessage?> TransitionLeasedAsync(Guid id, string leaseOwner, Func<InboxMessage, InboxMessage?> transition)
        {
            var db = Database;
            var hashEntries = await db.HashGetAllAsync(_keys.Message(id)).ConfigureAwait(false);
            if (hashEntries.Length == 0)
                return null;

            var existing = RedisInboxMessageMapper.FromHashEntries(hashEntries);
            if (existing.LeaseOwner != leaseOwner)
                return null;

            var updated = transition(existing);
            if (updated is null)
                return null;

            var version = RedisInboxMessageMapper.ReadVersion(hashEntries);
            var applied = await WriteAsync(db, id, updated, version + 1, casMode: "owner", casValue: leaseOwner).ConfigureAwait(false);
            return applied ? updated : null;
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default)
        {
            var db = Database;
            var now = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var ids = await db.SortedSetRangeByScoreAsync(_keys.Retryable(channelKey), double.NegativeInfinity, now, take: maxCount).ConfigureAwait(false);
            if (ids.Length == 0)
                return [];

            var messages = new List<InboxMessage>(ids.Length);
            foreach (var idValue in ids)
            {
                var entries = await db.HashGetAllAsync(_keys.Message(Guid.ParseExact((string)idValue!, "N"))).ConfigureAwait(false);
                if (entries.Length > 0)
                    messages.Add(RedisInboxMessageMapper.FromHashEntries(entries));
            }

            return messages;
        }

        /// <inheritdoc/>
        public async Task<int> PurgeAsync(Guid channelKey, DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default)
        {
            var db = Database;
            var terminalKey = _keys.Terminal(channelKey);
            var ids = await db.SortedSetRangeByScoreAsync(terminalKey, double.NegativeInfinity, olderThanUtc.ToUnixTimeMilliseconds() - 1).ConfigureAwait(false);
            if (ids.Length == 0)
                return 0;

            var result = await db.ScriptEvaluateAsync(PurgeScript, new
            {
                terminalKey = (RedisKey)terminalKey,
                keyPrefix = (RedisKey)_keys.MessagePrefix,
                idsJson = JsonSerializer.Serialize(ids.Select(v => (string)v!)),
            }).ConfigureAwait(false);

            return (int)result;
        }

        /// <inheritdoc/>
        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                var latency = await Database.PingAsync().ConfigureAwait(false);
                return HealthCheckResult.Healthy($"Redis responded to PING in {latency.TotalMilliseconds:F1}ms.");
            }
            catch (RedisConnectionException exception)
            {
                return HealthCheckResult.Unhealthy("Redis connection is unavailable.", exception);
            }
            catch (RedisTimeoutException exception)
            {
                return HealthCheckResult.Unhealthy("Redis did not respond to PING in time.", exception);
            }
        }
    }
}
