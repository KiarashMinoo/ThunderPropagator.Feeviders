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
    public sealed class RedisInboxStore : IInboxStore, IHealthCheck, IInboxStoreInitializer
    {
        /// <summary>
        /// Upper bound on how many times a version-CAS conflict during <see cref="TryClaimAsync"/>
        /// retries the read-decide-write cycle before giving up. Exists only to bound pathological
        /// contention (many workers claiming the exact same entry at once) - ordinary contention
        /// resolves in one or two attempts.
        /// </summary>
        private const int MaxCasAttempts = 20;

        /// <summary>
        /// This store's key/hash-field layout version - see <see cref="InitializeAsync"/>. Bump whenever
        /// a change here would make an older version of this class misread an existing keyspace (a
        /// renamed/removed hash field, a changed key-naming scheme, etc.).
        /// </summary>
        private const int RequiredSchemaVersion = 1;

        private const int MaxLockAcquireAttempts = 50;
        private static readonly TimeSpan LockAcquireRetryDelay = TimeSpan.FromMilliseconds(200);
        private static readonly TimeSpan LockTtl = TimeSpan.FromSeconds(30);

        private static readonly LuaScript ReleaseLockScript = LuaScript.Prepare(
            """
            if redis.call('GET', @lockKey) == @owner then
              return redis.call('DEL', @lockKey)
            end
            return 0
            """);

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
            if @terminalOp == 'remove' then
              redis.call('ZREM', @terminalKey, @id)
            elseif @terminalOp ~= 'none' then
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
            elseif @terminalOp == 'remove' then
              -- Leaving a terminal state (see InboxMessage.Replay) must clear any passive TTL a prior
              -- terminal write set on this key - a non-terminal entry must never expire out from under it.
              redis.call('PERSIST', @messageKey)
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

        /// <summary>
        /// Ensures this instance's <see cref="RequiredSchemaVersion"/> is compatible with whatever is
        /// already persisted at <see cref="RedisKeyNamespace.SchemaVersion"/>, the one key every replica
        /// touching this keyspace agrees to check/write. <see cref="StoreInitializationMode.Apply"/>
        /// serializes concurrent replicas via a <c>SET ... NX PX</c> mutual-exclusion lock at
        /// <see cref="RedisKeyNamespace.SchemaLock"/> (retried with a fixed delay up to
        /// <see cref="MaxLockAcquireAttempts"/> times, throwing <see cref="TimeoutException"/> if none
        /// ever succeeds - a genuinely exceptional, not-modeled-as-a-result condition, since it means
        /// another replica appears stuck rather than merely slow); <see cref="StoreInitializationMode.VerifyOnly"/>
        /// never mutates anything and therefore never needs the lock at all. There is no key/hash-field
        /// layout to actually migrate yet (version 1 is this backend's first-ever schema) - an upgrade
        /// today is only ever the version marker's own creation or bump; a future version bump that does
        /// change key/field layout would apply its migration steps here, before writing the new version.
        /// </summary>
        public async Task<StoreInitializationResult> InitializeAsync(StoreInitializationMode mode = StoreInitializationMode.Apply, CancellationToken cancellationToken = default)
        {
            var db = Database;

            if (mode == StoreInitializationMode.VerifyOnly)
                return await VerifySchemaAsync(db).ConfigureAwait(false);

            var owner = Guid.NewGuid().ToString("N");
            var acquired = false;
            for (var attempt = 0; attempt < MaxLockAcquireAttempts; attempt++)
            {
                acquired = await db.StringSetAsync(_keys.SchemaLock, owner, LockTtl, When.NotExists).ConfigureAwait(false);
                if (acquired)
                    break;

                await Task.Delay(LockAcquireRetryDelay, cancellationToken).ConfigureAwait(false);
            }

            if (!acquired)
                throw new TimeoutException(
                    $"Could not acquire the Inbox schema initialization lock '{_keys.SchemaLock}' within {MaxLockAcquireAttempts * LockAcquireRetryDelay.TotalSeconds:F0}s - another replica appears to be stuck holding it.");

            try
            {
                return await ApplySchemaAsync(db).ConfigureAwait(false);
            }
            finally
            {
                await db.ScriptEvaluateAsync(ReleaseLockScript, new { lockKey = (RedisKey)_keys.SchemaLock, owner }).ConfigureAwait(false);
            }
        }

        private async Task<StoreInitializationResult> ApplySchemaAsync(IDatabase db)
        {
            try
            {
                var persistedValue = await db.StringGetAsync(_keys.SchemaVersion).ConfigureAwait(false);

                if (persistedValue.IsNull)
                {
                    await db.StringSetAsync(_keys.SchemaVersion, RequiredSchemaVersion.ToString()).ConfigureAwait(false);
                    return Result(StoreInitializationOutcome.Upgraded, null, "Redis key/script layout initialized at version " + RequiredSchemaVersion + ".");
                }

                var persisted = (int)persistedValue;
                if (persisted == RequiredSchemaVersion)
                    return Result(StoreInitializationOutcome.Ready, persisted, "Redis key/script layout already at the required version.");

                if (persisted < RequiredSchemaVersion)
                {
                    await db.StringSetAsync(_keys.SchemaVersion, RequiredSchemaVersion.ToString()).ConfigureAwait(false);
                    return Result(StoreInitializationOutcome.Upgraded, persisted, $"Redis key/script layout upgraded from version {persisted} to {RequiredSchemaVersion}.");
                }

                return Result(StoreInitializationOutcome.IncompatibleVersion, persisted,
                    $"Persisted Redis key/script layout version {persisted} is newer than this code's version {RequiredSchemaVersion} - this code is too old to talk to this keyspace safely.");
            }
            catch (RedisException exception)
            {
                return Result(StoreInitializationOutcome.InsufficientPermissions, null, "Redis rejected a read/write needed to initialize the schema version key.", exception);
            }
        }

        private async Task<StoreInitializationResult> VerifySchemaAsync(IDatabase db)
        {
            try
            {
                var persistedValue = await db.StringGetAsync(_keys.SchemaVersion).ConfigureAwait(false);
                if (persistedValue.IsNull)
                    return Result(StoreInitializationOutcome.IncompatibleVersion, null,
                        "No Redis schema-version key found - this keyspace has never been initialized, and VerifyOnly mode never creates one.");

                var persisted = (int)persistedValue;
                return persisted == RequiredSchemaVersion
                    ? Result(StoreInitializationOutcome.VerifiedCompatible, persisted, "Redis key/script layout verified compatible.")
                    : Result(StoreInitializationOutcome.IncompatibleVersion, persisted, $"Persisted version {persisted} does not match required version {RequiredSchemaVersion}.");
            }
            catch (RedisException exception)
            {
                return Result(StoreInitializationOutcome.InsufficientPermissions, null, "Redis rejected the read needed to verify the schema version key.", exception);
            }
        }

        private static StoreInitializationResult Result(StoreInitializationOutcome outcome, int? persisted, string message, Exception? error = null) => new()
        {
            Outcome = outcome,
            RequiredSchemaVersion = RequiredSchemaVersion,
            PersistedSchemaVersion = persisted,
            Message = message,
            Error = error,
        };

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
        private async Task<bool> WriteAsync(IDatabase db, Guid id, InboxMessage updated, long newVersion, string casMode, string casValue, bool wasTerminal = false)
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
                // A write reaching a terminal state ZADDs the terminal zset; one leaving a terminal state
                // (only possible via InboxMessage.Replay - every other transition table entry only ever
                // reaches Processed/DeadLettered, never leaves them) must ZREM it instead of leaving a
                // stale entry behind.
                terminalOp = isTerminal ? terminalTimestamp!.Value.ToUnixTimeMilliseconds().ToString() : wasTerminal ? "remove" : "none",
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
        public async Task<InboxPurgeResult> PurgeAsync(InboxPurgeRequest request, CancellationToken cancellationToken = default)
        {
            if (request.ProcessedOlderThanUtc is null && request.DeadLetteredOlderThanUtc is null)
                return new InboxPurgeResult { PurgedCount = 0, HasMore = false };

            var db = Database;
            var terminalKey = _keys.Terminal(request.ChannelKey);

            // The terminal sorted set mixes Processed and DeadLettered members together, scored by
            // whichever one applies - bound the range scan by the LARGER of the two cutoffs (so a null
            // cutoff, meaning "skip this status", never widens the scan), then check each candidate's own
            // Status below to apply its OWN cutoff, since the shared score range alone cannot distinguish
            // them. A generous scan window (4x maxCount) makes it unlikely one batch under-counts when one
            // status vastly outnumbers the other in the same window; see InboxPurgeResult.HasMore's
            // conservative fallback for the rare case it still does.
            var maxCutoffMs = Math.Max(
                request.ProcessedOlderThanUtc?.ToUnixTimeMilliseconds() ?? long.MinValue,
                request.DeadLetteredOlderThanUtc?.ToUnixTimeMilliseconds() ?? long.MinValue) - 1;
            var scanWindow = Math.Max(request.MaxCount * 4, request.MaxCount + 1);

            var candidates = await db.SortedSetRangeByScoreAsync(terminalKey, double.NegativeInfinity, maxCutoffMs, take: scanWindow).ConfigureAwait(false);
            if (candidates.Length == 0)
                return new InboxPurgeResult { PurgedCount = 0, HasMore = false };

            var statusTasks = candidates
                .Select(candidate => db.HashGetAsync(_keys.MessagePrefix + (string)candidate!, RedisInboxMessageMapper.FieldStatus))
                .ToArray();
            await Task.WhenAll(statusTasks).ConfigureAwait(false);

            var matched = new List<string>(candidates.Length);
            for (var i = 0; i < candidates.Length; i++)
            {
                if (statusTasks[i].Result.IsNullOrEmpty)
                    continue; // already gone - a race with a concurrent purge/replay of the same entry

                var status = Enum.Parse<InboxMessageStatus>((string)statusTasks[i].Result!);
                var qualifies = status switch
                {
                    InboxMessageStatus.Processed => request.ProcessedOlderThanUtc is not null,
                    InboxMessageStatus.DeadLettered => request.DeadLetteredOlderThanUtc is not null,
                    _ => false,
                };

                if (qualifies)
                    matched.Add((string)candidates[i]!);
            }

            var hasMore = candidates.Length == scanWindow || matched.Count > request.MaxCount;
            var batch = matched.Count > request.MaxCount ? matched.Take(request.MaxCount) : matched;
            var idsToDelete = (request.ExcludedIds is { Count: > 0 }
                ? batch.Where(id => !request.ExcludedIds.Contains(Guid.ParseExact(id, "N")))
                : batch).ToArray();

            if (idsToDelete.Length == 0)
                return new InboxPurgeResult { PurgedCount = 0, HasMore = hasMore };

            var result = await db.ScriptEvaluateAsync(PurgeScript, new
            {
                terminalKey = (RedisKey)terminalKey,
                keyPrefix = (RedisKey)_keys.MessagePrefix,
                idsJson = JsonSerializer.Serialize(idsToDelete),
            }).ConfigureAwait(false);

            return new InboxPurgeResult { PurgedCount = (int)result, HasMore = hasMore };
        }

        /// <inheritdoc/>
        public async Task<InboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default)
        {
            var db = Database;

            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var hashEntries = await db.HashGetAllAsync(_keys.Message(id)).ConfigureAwait(false);
                if (hashEntries.Length == 0)
                    return null;

                var existing = RedisInboxMessageMapper.FromHashEntries(hashEntries);
                if (existing.Status is not (InboxMessageStatus.Processed or InboxMessageStatus.DeadLettered))
                    return null;

                var replayed = existing.Replay(_timeProvider);
                var version = RedisInboxMessageMapper.ReadVersion(hashEntries);
                var applied = await WriteAsync(db, id, replayed, version + 1, casMode: "version", casValue: version.ToString(), wasTerminal: true).ConfigureAwait(false);
                if (applied)
                    return replayed;
                // Else: another caller mutated this entry since our read above - retry the whole cycle.
            }

            throw new InvalidOperationException(
                $"Exceeded {MaxCasAttempts} attempts contending for an Inbox replay on entry '{id}' - this indicates pathological concurrency, not a normal outcome.");
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
