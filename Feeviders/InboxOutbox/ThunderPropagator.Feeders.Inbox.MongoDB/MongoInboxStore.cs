using Microsoft.Extensions.Diagnostics.HealthChecks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ThunderPropagator.Feeders.Inbox.MongoDB
{
    /// <summary>
    /// <see cref="IInboxStore"/> backed by <see href="https://www.mongodb.com/">MongoDB</see> via the
    /// official MongoDB.Driver. Durable across restarts as long as the MongoDB deployment itself
    /// persists.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Atomicity.</b> Every field-level decision (whether a claim is allowed, what the new state looks
    /// like) is computed in C# using <see cref="InboxMessage"/>'s own transition rules - exactly the same
    /// rules every other backend uses. A fresh claim races the collection's own unique index on
    /// (ChannelKey, PartitionKey, MessageId) via an insert that simply fails (caught and treated as
    /// "someone else already has this dedup key, go read it") if another caller won the race. A reclaim
    /// (an existing document's lease expired, or its retry delay elapsed) is protected by optimistic
    /// concurrency: a <see cref="InboxMessageDocument.Version"/> field folded into the same
    /// <c>replaceOne</c> filter as the document's own <c>_id</c>, so the write only applies if nothing
    /// else touched the document since it was read - MongoDB's single-document writes are always atomic,
    /// so this compare-and-swap needs no server-side scripting the way Redis's Lua-based CAS does. A
    /// conflict retries the whole read-decide-write cycle (bounded by <see cref="MaxCasAttempts"/>).
    /// </para>
    /// <para>
    /// <b>Null-safe uniqueness.</b> Unlike a SQL unique index (where <c>NULL &lt;&gt; NULL</c> lets two
    /// rows share an all-else-equal key as long as one column is null), MongoDB's unique index treats a
    /// missing/null indexed field as a single, shared value: at most one document may have
    /// <c>PartitionKey: null</c> for a given (ChannelKey, MessageId) pair. The EF Core backend needs a
    /// shadow, sentinel-substituted column to get this same guarantee on a relational engine; this
    /// backend needs no such workaround - the dedup index is built directly on the nullable
    /// <see cref="InboxMessageDocument.PartitionKey"/> field.
    /// </para>
    /// <para>
    /// <b>Document/field limits.</b> A single BSON document is capped at 16MB. This store's own payload
    /// ceiling (<see cref="InboxMessageLimits.MaxPayloadSizeBytes"/>, 1MB) plus its bounded header set
    /// (<see cref="InboxMessageLimits.MaxHeaderCount"/> entries) leaves every document comfortably under
    /// that ceiling with no additional enforcement needed here; an insert that somehow still exceeds it
    /// fails with the driver's own document-too-large error, which propagates uncaught like any other
    /// unexpected write failure.
    /// </para>
    /// <para>
    /// <b>Retention.</b> <see cref="PurgeAsync"/> is the primary retention mechanism, exactly like every
    /// other backend. <see cref="EnsureIndexesAsync"/> optionally also creates a native MongoDB TTL index
    /// on <see cref="InboxMessageDocument.TerminalAtUtc"/> - a passive safety net if <see cref="PurgeAsync"/>
    /// is never called. That field is set only once an entry reaches a terminal state (Processed or
    /// DeadLettered - see <see cref="InboxMessageMapper.ToDocument"/>), and MongoDB's TTL index silently
    /// ignores a document whose indexed field is missing/null, so a Received/Processing/Failed entry can
    /// never expire out from under an in-flight claim or an unretried failure, however long that takes.
    /// </para>
    /// <para>
    /// <b>Indexes are not created automatically.</b> Call <see cref="EnsureIndexesAsync"/> once at
    /// startup (idempotent - safe to call every time the process starts) before using this store; nothing
    /// here creates the dedup/status/TTL indexes lazily on first use, the same way the EF Core backend
    /// relies on a migration having already run.
    /// </para>
    /// <para>
    /// <b>Reconnects.</b> This store does not itself manage connectivity - it uses whatever
    /// <see cref="IMongoDatabase"/> the caller supplies, and the driver's own topology
    /// monitoring/automatic-retry behavior (<c>retryWrites</c>/<c>retryReads</c>, both enabled by default)
    /// applies as configured on the underlying <see cref="IMongoClient"/>. A <see cref="MongoException"/>
    /// during any operation propagates to the caller uncaught - a relay/retry worker's own existing
    /// exception handling already treats an unexpected exception as transient and retries with backoff.
    /// </para>
    /// </remarks>
    public sealed class MongoInboxStore : IInboxStore, IHealthCheck
    {
        /// <summary>Default collection name - see <see cref="MongoInboxStoreServiceCollectionExtensions.AddMongoInboxStore"/> for overriding it.</summary>
        public const string DefaultCollectionName = "__TP_Inbox";

        /// <summary>
        /// Upper bound on how many times an optimistic-concurrency conflict during <see cref="TryClaimAsync"/>
        /// retries the read-decide-write cycle before giving up. Exists only to bound pathological
        /// contention (many workers claiming the exact same entry at once) - ordinary contention resolves
        /// in one or two attempts.
        /// </summary>
        private const int MaxCasAttempts = 20;

        private readonly IMongoCollection<InboxMessageDocument> _collection;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan? _terminalEntryTtl;

        /// <param name="database">Owned by the caller - never disposed by this store.</param>
        /// <param name="collectionName">Collection this instance reads/writes. Defaults to <see cref="DefaultCollectionName"/>; override to run more than one named store against the same database.</param>
        /// <param name="timeProvider">Clock used for timestamps and lease/retry expiry. Defaults to <see cref="TimeProvider.System"/>.</param>
        /// <param name="terminalEntryTtl">Passive-safety-net TTL applied once an entry becomes terminal - see the class remarks. <see langword="null"/> (default) disables it, relying solely on explicit <see cref="PurgeAsync"/> calls. Only takes effect once <see cref="EnsureIndexesAsync"/> has run.</param>
        public MongoInboxStore(IMongoDatabase database, string collectionName = DefaultCollectionName, TimeProvider? timeProvider = null, TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(database);
            ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);

            _collection = database.GetCollection<InboxMessageDocument>(collectionName);
            _timeProvider = timeProvider ?? TimeProvider.System;
            _terminalEntryTtl = terminalEntryTtl;
        }

        /// <summary>
        /// Creates the dedup-uniqueness, status-lookup, and (when a <c>terminalEntryTtl</c> was supplied)
        /// TTL indexes this store depends on. Idempotent - <c>createIndexes</c> is a no-op for an index
        /// that already exists with the same specification, so this is safe to call every time the
        /// process starts, not just once ever.
        /// </summary>
        public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
        {
            var models = new List<CreateIndexModel<InboxMessageDocument>>
            {
                new(Builders<InboxMessageDocument>.IndexKeys
                        .Ascending(x => x.ChannelKey).Ascending(x => x.PartitionKey).Ascending(x => x.MessageId),
                    new CreateIndexOptions { Name = "ux_dedup", Unique = true }),
                new(Builders<InboxMessageDocument>.IndexKeys.Ascending(x => x.ChannelKey).Ascending(x => x.Status),
                    new CreateIndexOptions { Name = "ix_channel_status" }),
            };

            if (_terminalEntryTtl is { } ttl)
            {
                models.Add(new CreateIndexModel<InboxMessageDocument>(
                    Builders<InboxMessageDocument>.IndexKeys.Ascending(x => x.TerminalAtUtc),
                    new CreateIndexOptions { Name = "ttl_terminal", ExpireAfter = ttl }));
            }

            await _collection.Indexes.CreateManyAsync(models, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<InboxClaimResult> TryClaimAsync(InboxClaimRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);

            if (await TryInsertAsync(request, cancellationToken).ConfigureAwait(false) is { } created)
                return InboxClaimResult.Claimed(created);

            var dedupFilter = DedupFilter(request.ChannelKey, request.PartitionKey, request.MessageId);

            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var existingDoc = await _collection.Find(dedupFilter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                if (existingDoc is null)
                {
                    // The entry that won the insert race was purged before we could read it back -
                    // vanishingly rare, but the correct response is simply to try creating it again.
                    if (await TryInsertAsync(request, cancellationToken).ConfigureAwait(false) is { } recreated)
                        return InboxClaimResult.Claimed(recreated);
                    continue;
                }

                var existing = InboxMessageMapper.ToDomain(existingDoc);
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

                if (await TryReplaceAsync(existingDoc.Id, existingDoc.Version, reclaimed, cancellationToken).ConfigureAwait(false))
                    return InboxClaimResult.Claimed(reclaimed);
                // Else: someone else won the race since our read above - retry the whole cycle.
            }

            throw new InvalidOperationException(
                $"Exceeded {MaxCasAttempts} attempts contending for an Inbox claim on message '{request.MessageId}' - this indicates pathological concurrency, not a normal outcome.");
        }

        private async Task<InboxMessage?> TryInsertAsync(InboxClaimRequest request, CancellationToken cancellationToken)
        {
            var now = _timeProvider.GetUtcNow();
            var received = InboxMessage.CreateReceived(
                Guid.NewGuid(), request.MessageId, request.ChannelKey, request.FeederId, request.SchemaVersion,
                request.PayloadContentType, request.Payload, request.Headers, request.PartitionKey, _timeProvider);
            var claimed = received.TryTransitionTo(
                InboxMessageStatus.Processing, _timeProvider,
                leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

            try
            {
                await _collection.InsertOneAsync(InboxMessageMapper.ToDocument(claimed, version: 1), cancellationToken: cancellationToken).ConfigureAwait(false);
                return claimed;
            }
            catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
            {
                return null;
            }
        }

        private async Task<bool> TryReplaceAsync(string id, long expectedVersion, InboxMessage updated, CancellationToken cancellationToken)
        {
            var filter = Builders<InboxMessageDocument>.Filter.Eq(x => x.Id, id) &
                         Builders<InboxMessageDocument>.Filter.Eq(x => x.Version, expectedVersion);
            var replacement = InboxMessageMapper.ToDocument(updated, expectedVersion + 1);

            var result = await _collection.ReplaceOneAsync(filter, replacement, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.MatchedCount == 1;
        }

        private static FilterDefinition<InboxMessageDocument> DedupFilter(Guid channelKey, string? partitionKey, string messageId) =>
            Builders<InboxMessageDocument>.Filter.Eq(x => x.ChannelKey, channelKey.ToString("N")) &
            Builders<InboxMessageDocument>.Filter.Eq(x => x.PartitionKey, partitionKey) &
            Builders<InboxMessageDocument>.Filter.Eq(x => x.MessageId, messageId);

        /// <inheritdoc/>
        public async Task<InboxMessage?> GetAsync(string messageId, Guid channelKey, string? partitionKey, CancellationToken cancellationToken = default)
        {
            var doc = await _collection.Find(DedupFilter(channelKey, partitionKey, messageId)).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            return doc is null ? null : InboxMessageMapper.ToDomain(doc);
        }

        /// <inheritdoc/>
        public Task<InboxMessage?> CompleteAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Processed, _timeProvider), cancellationToken);

        /// <inheritdoc/>
        public Task<InboxMessage?> FailAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Failed, _timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason), cancellationToken);

        /// <inheritdoc/>
        public Task<InboxMessage?> DeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.DeadLettered, _timeProvider, failureReason: failureReason), cancellationToken);

        /// <inheritdoc/>
        public Task<InboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.RenewLease(_timeProvider, leaseOwner, leaseExtension), cancellationToken);

        private async Task<InboxMessage?> TransitionLeasedAsync(Guid id, string leaseOwner, Func<InboxMessage, InboxMessage?> transition, CancellationToken cancellationToken)
        {
            var idString = id.ToString("N");
            var doc = await _collection.Find(x => x.Id == idString).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (doc is null)
                return null;

            var existing = InboxMessageMapper.ToDomain(doc);
            if (existing.LeaseOwner != leaseOwner)
                return null;

            var updated = transition(existing);
            if (updated is null)
                return null;

            return await TryReplaceAsync(idString, doc.Version, updated, cancellationToken).ConfigureAwait(false) ? updated : null;
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default)
        {
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            var channelKeyString = channelKey.ToString("N");
            var builder = Builders<InboxMessageDocument>.Filter;
            var filter = builder.Eq(x => x.ChannelKey, channelKeyString) & (
                (builder.Eq(x => x.Status, InboxMessageStatus.Failed) &
                 (builder.Eq(x => x.NextRetryAtUtc, null) | builder.Lte(x => x.NextRetryAtUtc, now)))
                |
                (builder.Eq(x => x.Status, InboxMessageStatus.Processing) &
                 (builder.Eq(x => x.LeaseExpiresAtUtc, null) | builder.Lte(x => x.LeaseExpiresAtUtc, now)))
            );

            var docs = await _collection.Find(filter).Limit(maxCount).ToListAsync(cancellationToken).ConfigureAwait(false);
            return docs.Select(InboxMessageMapper.ToDomain).ToArray();
        }

        /// <inheritdoc/>
        public async Task<int> PurgeAsync(Guid channelKey, DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default)
        {
            var channelKeyString = channelKey.ToString("N");
            var builder = Builders<InboxMessageDocument>.Filter;
            var filter = builder.Eq(x => x.ChannelKey, channelKeyString) &
                         builder.In(x => x.Status, [InboxMessageStatus.Processed, InboxMessageStatus.DeadLettered]) &
                         builder.Lt(x => x.TerminalAtUtc, olderThanUtc.UtcDateTime);

            var result = await _collection.DeleteManyAsync(filter, cancellationToken).ConfigureAwait(false);
            return (int)result.DeletedCount;
        }

        /// <inheritdoc/>
        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                await _collection.Database.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1), cancellationToken: cancellationToken).ConfigureAwait(false);
                return HealthCheckResult.Healthy("MongoDB responded to ping.");
            }
            catch (MongoException exception)
            {
                return HealthCheckResult.Unhealthy("MongoDB is unavailable.", exception);
            }
        }
    }
}
