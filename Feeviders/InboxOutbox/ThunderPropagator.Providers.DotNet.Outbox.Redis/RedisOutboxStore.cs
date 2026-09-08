using System.Text.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace ThunderPropagator.Providers.DotNet.Outbox.Redis
{
    /// <summary>
    /// <see cref="IOutboxStore"/> backed by <see href="https://redis.io/">Redis</see> via
    /// StackExchange.Redis. Durable across restarts as long as the Redis deployment itself persists
    /// (RDB/AOF) - unlike <c>InMemoryOutboxStore</c>, an entry survives this process exiting. Cannot
    /// participate in <see cref="OutboxTransactionMode.Enlisted"/> (Redis has no shared local
    /// transaction with an arbitrary business-side relational store) - <see cref="OutboxOptions.Validate"/>
    /// already rejects that combination for <see cref="OutboxStoreType.Redis"/> at configuration time,
    /// so this store never even receives an enlisted claim to reject itself.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Atomicity.</b> Every field-level decision is computed in C# using <see cref="OutboxMessage"/>'s
    /// own transition rules - exactly the same rules <c>InMemoryOutboxStore</c> uses. Redis's role is
    /// purely to make the read-decide-write sequence atomic via a single Lua script per mutation, keyed
    /// on either the entry's monotonic <c>Version</c> field (<see cref="ClaimBatchAsync"/>, where there
    /// is no prior owner identity to check) or its current <c>LeaseOwner</c> (every other lease-scoped
    /// mutation). A version-CAS conflict during a batch claim simply skips that one candidate rather
    /// than retrying or failing the whole batch - another caller already claimed or otherwise mutated it,
    /// so it is no longer this call's to claim.
    /// </para>
    /// <para>
    /// <b>Keys.</b> Every key this instance touches shares one Redis Cluster hash tag (the
    /// <c>keyPrefix</c> constructor argument, wrapped in <c>{}</c>) so a multi-key Lua script never spans
    /// more than one cluster slot - see <see cref="RedisKeyNamespace"/>. Two stores sharing one Redis
    /// database/cluster MUST use different <c>keyPrefix</c>es or their keyspaces will collide.
    /// </para>
    /// <para>
    /// <b>Retention.</b> <see cref="PurgeAsync"/> is the primary retention mechanism, exactly like
    /// <c>InMemoryOutboxStore</c>. Optionally, <c>terminalEntryTtl</c> additionally applies a
    /// native Redis <c>EXPIRE</c> to an entry's hash key the moment it becomes terminal (Published or
    /// DeadLettered) - a passive safety net if <see cref="PurgeAsync"/> is never called - but never to a
    /// non-terminal entry: a Pending/Publishing/Failed entry's key never has a TTL set on it, so it can
    /// never expire out from under an in-flight claim or an unretried failure.
    /// </para>
    /// <para>
    /// <b>Reconnects/timeouts/cluster.</b> This store does not itself manage reconnection - it uses
    /// whatever <see cref="IConnectionMultiplexer"/> the caller supplies, and StackExchange.Redis's own
    /// automatic-reconnect behavior applies exactly as the caller configured it. A
    /// <see cref="RedisConnectionException"/>/<see cref="RedisTimeoutException"/> during any operation
    /// propagates uncaught - <c>OutboxRelayWorker</c>'s existing exception handling already treats an
    /// unexpected exception as transient and retries with backoff. Every script uses
    /// <see cref="StackExchange.Redis.LuaScript"/>, which transparently falls back from <c>EVALSHA</c> to
    /// a full <c>EVAL</c> on a cache miss (e.g. after failover to a replica that never had the script
    /// loaded) - no manual script-cache management needed.
    /// </para>
    /// <para>
    /// <b>Not throughput-optimized.</b> Like <c>InMemoryOutboxStore</c>, this backend favors correctness
    /// and auditability over raw throughput: <see cref="ClaimBatchAsync"/> scans a partition's entire
    /// active set rather than a bounded window, and <see cref="GetClaimablePartitionKeysAsync"/>/
    /// <see cref="GetOldestPendingAgeAsync"/> may inspect every known partition's entries.
    /// </para>
    /// </remarks>
    public sealed class RedisOutboxStore : IOutboxStore, IHealthCheck, IOutboxStoreInitializer
    {
        /// <summary>
        /// This store's key/hash-field layout version - see <see cref="InitializeAsync"/>. Bump whenever
        /// a change here would make an older version of this class misread an existing keyspace.
        /// </summary>
        private const int RequiredSchemaVersion = 1;

        /// <summary>
        /// Upper bound on how many times a version-CAS conflict during <see cref="ReplayAsync"/> retries
        /// the read-decide-write cycle before giving up. Exists only to bound pathological contention
        /// (many callers requeuing the exact same entry at once) - ordinary contention resolves in one
        /// or two attempts.
        /// </summary>
        private const int MaxCasAttempts = 20;

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
        // travels as its own separate, natively binary-safe scalar - see RedisOutboxMessageMapper's remarks.
        private static readonly LuaScript EnqueueFinalizeScript = LuaScript.Prepare(
            """
            redis.call('SADD', @knownPartitionsKey, @partitionSlot)
            redis.call('ZADD', @activeKey, @orderingSequence, @id)
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
            if @terminal == '1' then
              redis.call('ZREM', @activeKey, @id)
              if redis.call('ZCARD', @activeKey) == 0 then
                redis.call('SREM', @knownPartitionsKey, @partitionSlot)
              end
              redis.call('ZADD', @terminalKey, @terminalScore, @id)
            elseif @reactivate == '1' then
              -- Leaving a terminal state (see OutboxMessage.Requeue) - the mirror image of the branch
              -- above: back onto the active set (and known-partitions, in case this was the partition's
              -- only entry and therefore fell out of it) at its freshly assigned OrderingSequence, off
              -- the terminal set.
              redis.call('ZREM', @terminalKey, @id)
              redis.call('SADD', @knownPartitionsKey, @partitionSlot)
              redis.call('ZADD', @activeKey, @activeScore, @id)
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
            elseif @reactivate == '1' then
              -- A non-terminal entry must never expire out from under it - clear whatever passive TTL a
              -- prior terminal write set on this key.
              redis.call('PERSIST', @messageKey)
            end
            return 1
            """);

        private static readonly LuaScript ReleaseScript = LuaScript.Prepare(
            """
            local currentOwner = redis.call('HGET', @messageKey, 'LeaseOwner')
            local status = redis.call('HGET', @messageKey, 'Status')
            if currentOwner ~= @leaseOwner or status ~= 'Publishing' then
              return 0
            end
            redis.call('HSET', @messageKey, 'Status', 'Pending')
            redis.call('HDEL', @messageKey, 'LeaseOwner', 'LeaseExpiresAtUtc', 'PublishingStartedAtUtc')
            return 1
            """);

        private static readonly LuaScript PurgeScript = LuaScript.Prepare(
            """
            local ids = cjson.decode(@idsJson)
            for i = 1, #ids do
              redis.call('DEL', @keyPrefix .. ids[i])
            end
            if #ids > 0 then
              redis.call('ZREM', @terminalKey, unpack(ids))
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
        public RedisOutboxStore(IConnectionMultiplexer connectionMultiplexer, string keyPrefix, TimeProvider? timeProvider = null, TimeSpan? terminalEntryTtl = null)
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
        /// already persisted at <see cref="RedisKeyNamespace.SchemaVersion"/> - see <c>RedisInboxStore.InitializeAsync</c>'s
        /// remarks for the full design (mutual-exclusion lock for <see cref="StoreInitializationMode.Apply"/>,
        /// no lock needed for <see cref="StoreInitializationMode.VerifyOnly"/>, nothing to actually
        /// migrate yet since version 1 is this backend's first-ever schema).
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
                    $"Could not acquire the Outbox schema initialization lock '{_keys.SchemaLock}' within {MaxLockAcquireAttempts * LockAcquireRetryDelay.TotalSeconds:F0}s - another replica appears to be stuck holding it.");

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
        public async Task<OutboxMessage> EnqueueAsync(OutboxEnqueueRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            var db = Database;
            var partitionSlot = RedisKeyNamespace.PartitionSlot(request.PartitionKey);

            // INCR alone is already atomic and durably reserves this sequence number process-wide - a
            // crash before the finalize script below runs only ever leaves a skipped (never reused)
            // sequence number, not a duplicate or a lost one.
            var orderingSequence = await db.StringIncrementAsync(_keys.OrderingSequence(partitionSlot)).ConfigureAwait(false) - 1;

            var message = OutboxMessage.CreatePending(
                Guid.NewGuid(), request.MessageId, request.ProviderKey, orderingSequence,
                request.SchemaVersion, request.PayloadContentType, request.Payload,
                request.Headers, request.PartitionKey, _timeProvider);

            var fieldsJson = RedisOutboxMessageMapper.ToFullFieldListJson(message, version: 1);
            await db.ScriptEvaluateAsync(EnqueueFinalizeScript, new
            {
                knownPartitionsKey = (RedisKey)_keys.KnownPartitions,
                activeKey = (RedisKey)_keys.Active(partitionSlot),
                messageKey = (RedisKey)_keys.Message(message.Id),
                partitionSlot,
                id = message.Id.ToString("N"),
                orderingSequence,
                payload = (RedisValue)message.Payload,
                fieldsJson,
            }).ConfigureAwait(false);

            return message;
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string? partitionKey, int maxCount, string leaseOwner, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            var db = Database;
            var partitionSlot = RedisKeyNamespace.PartitionSlot(partitionKey);
            var candidateIds = await db.SortedSetRangeByScoreAsync(_keys.Active(partitionSlot)).ConfigureAwait(false);
            if (candidateIds.Length == 0)
                return [];

            var claimed = new List<OutboxMessage>();
            foreach (var idValue in candidateIds)
            {
                if (claimed.Count >= maxCount)
                    break;

                var id = Guid.ParseExact((string)idValue!, "N");
                var hashEntries = await db.HashGetAllAsync(_keys.Message(id)).ConfigureAwait(false);
                if (hashEntries.Length == 0)
                    continue; // Purged/removed concurrently since the ZRANGE snapshot was taken.

                var existing = RedisOutboxMessageMapper.FromHashEntries(hashEntries);
                var now = _timeProvider.GetUtcNow();
                if (!IsClaimable(existing, now))
                    continue;

                var version = RedisOutboxMessageMapper.ReadVersion(hashEntries);
                // An abandoned Publishing lease must first release back through Failed (Publishing isn't
                // a valid source for Publishing in OutboxMessage's transition table) before it can be
                // reclaimed. Every claim - first attempt, reclaimed abandoned lease, or retry - is
                // itself a new attempt, so increments unconditionally.
                var source = existing.Status == OutboxMessageStatus.Publishing
                    ? existing.TryTransitionTo(OutboxMessageStatus.Failed, _timeProvider)
                    : existing;
                var publishing = source.TryTransitionTo(
                    OutboxMessageStatus.Publishing, _timeProvider,
                    leaseOwner: leaseOwner, leaseExpiresAtUtc: now + leaseDuration, incrementAttempt: true);

                if (await WriteAsync(db, id, publishing, version + 1, partitionSlot, casMode: "version", casValue: version.ToString()).ConfigureAwait(false))
                    claimed.Add(publishing);
                // Else: another caller claimed/mutated this entry since our read above - it is no
                // longer this call's to claim, so move on to the next candidate rather than retrying.
            }

            return claimed;
        }

        private static bool IsClaimable(OutboxMessage message, DateTimeOffset now) => message.Status switch
        {
            OutboxMessageStatus.Pending => true,
            OutboxMessageStatus.Publishing => message.LeaseExpiresAtUtc is null || message.LeaseExpiresAtUtc <= now,
            OutboxMessageStatus.Failed => message.NextRetryAtUtc is null || message.NextRetryAtUtc <= now,
            _ => false,
        };

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string?>> GetClaimablePartitionKeysAsync(CancellationToken cancellationToken = default)
        {
            var db = Database;
            var slots = await db.SetMembersAsync(_keys.KnownPartitions).ConfigureAwait(false);
            var now = _timeProvider.GetUtcNow();
            var claimable = new List<string?>();

            foreach (var slotValue in slots)
            {
                var slot = (string)slotValue!;
                var ids = await db.SortedSetRangeByScoreAsync(_keys.Active(slot)).ConfigureAwait(false);
                foreach (var idValue in ids)
                {
                    var entries = await db.HashGetAllAsync(_keys.Message(Guid.ParseExact((string)idValue!, "N"))).ConfigureAwait(false);
                    if (entries.Length > 0 && IsClaimable(RedisOutboxMessageMapper.FromHashEntries(entries), now))
                    {
                        claimable.Add(slot == RedisKeyNamespace.PartitionSlot(null) ? null : slot);
                        break;
                    }
                }
            }

            return claimable;
        }

        /// <summary>Applies a CAS-guarded write, including its Active/Terminal/KnownPartitions side effects for a terminal transition (or, when <paramref name="wasTerminal"/>, a reactivating one) and the passive TTL.</summary>
        private async Task<bool> WriteAsync(IDatabase db, Guid id, OutboxMessage updated, long newVersion, string partitionSlot, string casMode, string casValue, bool wasTerminal = false)
        {
            var (setFieldsJson, deleteFieldsJson) = RedisOutboxMessageMapper.ToChangedFieldListJson(updated, newVersion);
            var isTerminal = updated.Status is OutboxMessageStatus.Published or OutboxMessageStatus.DeadLettered;
            var terminalTimestamp = updated.Status switch
            {
                OutboxMessageStatus.Published => updated.PublishedAtUtc,
                OutboxMessageStatus.DeadLettered => updated.DeadLetteredAtUtc,
                _ => (DateTimeOffset?)null,
            };

            var result = (int)await db.ScriptEvaluateAsync(WriteScript, new
            {
                messageKey = (RedisKey)_keys.Message(id),
                activeKey = (RedisKey)_keys.Active(partitionSlot),
                knownPartitionsKey = (RedisKey)_keys.KnownPartitions,
                terminalKey = (RedisKey)_keys.Terminal,
                partitionSlot,
                id = id.ToString("N"),
                casMode,
                casValue,
                terminal = isTerminal ? "1" : "0",
                terminalScore = isTerminal ? terminalTimestamp!.Value.ToUnixTimeMilliseconds() : 0,
                reactivate = !isTerminal && wasTerminal ? "1" : "0",
                activeScore = updated.OrderingSequence,
                setFieldsJson,
                deleteFieldsJson,
                ttlSeconds = isTerminal && _terminalEntryTtl is { } ttl ? ((long)ttl.TotalSeconds).ToString() : "",
            }).ConfigureAwait(false);

            return result == 1;
        }

        /// <inheritdoc/>
        public Task<OutboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.RenewLease(_timeProvider, leaseOwner, leaseExtension));

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkPublishedAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Published, _timeProvider));

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkFailedAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Failed, _timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason));

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkDeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.DeadLettered, _timeProvider, failureReason: failureReason));

        private async Task<OutboxMessage?> TransitionLeasedAsync(Guid id, string leaseOwner, Func<OutboxMessage, OutboxMessage?> transition)
        {
            var db = Database;
            var hashEntries = await db.HashGetAllAsync(_keys.Message(id)).ConfigureAwait(false);
            if (hashEntries.Length == 0)
                return null;

            var existing = RedisOutboxMessageMapper.FromHashEntries(hashEntries);
            if (existing.LeaseOwner != leaseOwner)
                return null;

            var updated = transition(existing);
            if (updated is null)
                return null;

            var version = RedisOutboxMessageMapper.ReadVersion(hashEntries);
            var partitionSlot = RedisKeyNamespace.PartitionSlot(existing.PartitionKey);
            var applied = await WriteAsync(db, id, updated, version + 1, partitionSlot, casMode: "owner", casValue: leaseOwner).ConfigureAwait(false);
            return applied ? updated : null;
        }

        /// <inheritdoc/>
        public async Task<bool> ReleaseAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default)
        {
            var result = (int)await Database.ScriptEvaluateAsync(ReleaseScript, new
            {
                messageKey = (RedisKey)_keys.Message(id),
                leaseOwner,
            }).ConfigureAwait(false);

            return result == 1;
        }

        /// <inheritdoc/>
        public async Task<int> GetDepthAsync(string? partitionKey, CancellationToken cancellationToken = default)
        {
            var db = Database;

            if (partitionKey is not null)
                return (int)await db.SortedSetLengthAsync(_keys.Active(RedisKeyNamespace.PartitionSlot(partitionKey))).ConfigureAwait(false);

            var slots = await db.SetMembersAsync(_keys.KnownPartitions).ConfigureAwait(false);
            var total = 0;
            foreach (var slot in slots)
                total += (int)await db.SortedSetLengthAsync(_keys.Active((string)slot!)).ConfigureAwait(false);

            return total;
        }

        /// <inheritdoc/>
        public async Task<TimeSpan?> GetOldestPendingAgeAsync(string? partitionKey, TimeProvider ageTimeProvider, CancellationToken cancellationToken = default)
        {
            var db = Database;
            var slots = partitionKey is not null
                ? [RedisKeyNamespace.PartitionSlot(partitionKey)]
                : (await db.SetMembersAsync(_keys.KnownPartitions).ConfigureAwait(false)).Select(s => (string)s!).ToArray();

            DateTimeOffset? oldest = null;
            foreach (var slot in slots)
            {
                var ids = await db.SortedSetRangeByScoreAsync(_keys.Active(slot)).ConfigureAwait(false);
                foreach (var idValue in ids)
                {
                    var createdAt = await db.HashGetAsync(_keys.Message(Guid.ParseExact((string)idValue!, "N")), RedisOutboxMessageMapper.FieldCreatedAtUtc).ConfigureAwait(false);
                    if (createdAt.IsNull)
                        continue;

                    var timestamp = DateTimeOffset.FromUnixTimeMilliseconds((long)createdAt);
                    if (oldest is null || timestamp < oldest)
                        oldest = timestamp;
                }
            }

            return oldest is null ? null : ageTimeProvider.GetUtcNow() - oldest.Value;
        }

        /// <inheritdoc/>
        public async Task<OutboxPurgeResult> PurgeAsync(OutboxPurgeRequest request, CancellationToken cancellationToken = default)
        {
            if (request.PublishedOlderThanUtc is null && request.DeadLetteredOlderThanUtc is null)
                return new OutboxPurgeResult { PurgedCount = 0, HasMore = false };

            var db = Database;

            // The terminal sorted set mixes Published and DeadLettered members together, scored by
            // whichever one applies - bound the range scan by the LARGER of the two cutoffs (so a null
            // cutoff, meaning "skip this status", never widens the scan), then check each candidate's own
            // Status below to apply its OWN cutoff, since the shared score range alone cannot distinguish
            // them. A generous scan window (4x maxCount) makes it unlikely one batch under-counts when one
            // status vastly outnumbers the other in the same window; see OutboxPurgeResult.HasMore's
            // conservative fallback for the rare case it still does.
            var maxCutoffMs = Math.Max(
                request.PublishedOlderThanUtc?.ToUnixTimeMilliseconds() ?? long.MinValue,
                request.DeadLetteredOlderThanUtc?.ToUnixTimeMilliseconds() ?? long.MinValue) - 1;
            var scanWindow = Math.Max(request.MaxCount * 4, request.MaxCount + 1);

            var candidates = await db.SortedSetRangeByScoreAsync(_keys.Terminal, double.NegativeInfinity, maxCutoffMs, take: scanWindow).ConfigureAwait(false);
            if (candidates.Length == 0)
                return new OutboxPurgeResult { PurgedCount = 0, HasMore = false };

            var statusTasks = candidates
                .Select(candidate => db.HashGetAsync(_keys.MessagePrefix + (string)candidate!, RedisOutboxMessageMapper.FieldStatus))
                .ToArray();
            await Task.WhenAll(statusTasks).ConfigureAwait(false);

            var matched = new List<string>(candidates.Length);
            for (var i = 0; i < candidates.Length; i++)
            {
                if (statusTasks[i].Result.IsNullOrEmpty)
                    continue; // already gone - a race with a concurrent purge/replay of the same entry

                var status = Enum.Parse<OutboxMessageStatus>((string)statusTasks[i].Result!);
                var qualifies = status switch
                {
                    OutboxMessageStatus.Published => request.PublishedOlderThanUtc is not null,
                    OutboxMessageStatus.DeadLettered => request.DeadLetteredOlderThanUtc is not null,
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
                return new OutboxPurgeResult { PurgedCount = 0, HasMore = hasMore };

            var result = await db.ScriptEvaluateAsync(PurgeScript, new
            {
                terminalKey = (RedisKey)_keys.Terminal,
                keyPrefix = (RedisKey)_keys.MessagePrefix,
                idsJson = JsonSerializer.Serialize(idsToDelete),
            }).ConfigureAwait(false);

            return new OutboxPurgeResult { PurgedCount = (int)result, HasMore = hasMore };
        }

        /// <inheritdoc/>
        public async Task<OutboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default)
        {
            var db = Database;

            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var hashEntries = await db.HashGetAllAsync(_keys.Message(id)).ConfigureAwait(false);
                if (hashEntries.Length == 0)
                    return null;

                var existing = RedisOutboxMessageMapper.FromHashEntries(hashEntries);
                if (existing.Status is not (OutboxMessageStatus.Published or OutboxMessageStatus.DeadLettered))
                    return null;

                var partitionSlot = RedisKeyNamespace.PartitionSlot(existing.PartitionKey);
                // INCR alone is already atomic and durably reserves this sequence number - see
                // EnqueueAsync's own remarks on why a crash here only ever skips a value, never
                // duplicates or loses one.
                var newOrderingSequence = await db.StringIncrementAsync(_keys.OrderingSequence(partitionSlot)).ConfigureAwait(false) - 1;
                var requeued = existing.Requeue(newOrderingSequence, _timeProvider);

                var version = RedisOutboxMessageMapper.ReadVersion(hashEntries);
                var applied = await WriteAsync(db, id, requeued, version + 1, partitionSlot, casMode: "version", casValue: version.ToString(), wasTerminal: true).ConfigureAwait(false);
                if (applied)
                    return requeued;
                // Else: another caller mutated this entry since our read above - retry the whole cycle
                // (the just-allocated ordering sequence is simply skipped, never reused or duplicated).
            }

            throw new InvalidOperationException(
                $"Exceeded {MaxCasAttempts} attempts contending for an Outbox replay on entry '{id}' - this indicates pathological concurrency, not a normal outcome.");
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
