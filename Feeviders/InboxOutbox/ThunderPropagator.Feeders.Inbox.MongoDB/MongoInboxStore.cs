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
    /// <b>Indexes/schema are not created automatically.</b> Call <see cref="InitializeAsync"/> once at
    /// startup (idempotent and safe under concurrent replicas - see its own remarks) before using this
    /// store; nothing here creates the dedup/status/TTL indexes or the schema-version document lazily on
    /// first use, the same way the EF Core backend relies on its own initializer having already run.
    /// <see cref="EnsureIndexesAsync"/> remains available directly for a caller that only wants the
    /// indexes (e.g. a test fixture) without the schema-version bookkeeping <see cref="InitializeAsync"/>
    /// also does.
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
    public sealed class MongoInboxStore : IInboxStore, IHealthCheck, IInboxStoreInitializer
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

        /// <summary>Default schema-version metadata collection name - see <see cref="MongoInboxStoreServiceCollectionExtensions.AddMongoInboxStore"/> for overriding it.</summary>
        public const string DefaultMetaCollectionName = "__TP_Inbox_Meta";

        /// <summary>
        /// This store's document-shape/index layout version - see <see cref="InitializeAsync"/>. Bump
        /// whenever a change here would make an older version of this class misread existing documents.
        /// </summary>
        private const int RequiredSchemaVersion = 1;

        private const string SchemaDocId = "schema";

        private readonly IMongoCollection<InboxMessageDocument> _collection;
        private readonly IMongoCollection<SchemaVersionDocument> _metaCollection;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan? _terminalEntryTtl;

        /// <param name="database">Owned by the caller - never disposed by this store.</param>
        /// <param name="collectionName">Collection this instance reads/writes. Defaults to <see cref="DefaultCollectionName"/>; override to run more than one named store against the same database.</param>
        /// <param name="metaCollectionName">Schema-version metadata collection - see <see cref="InitializeAsync"/>. Defaults to <see cref="DefaultMetaCollectionName"/>.</param>
        /// <param name="timeProvider">Clock used for timestamps and lease/retry expiry. Defaults to <see cref="TimeProvider.System"/>.</param>
        /// <param name="terminalEntryTtl">Passive-safety-net TTL applied once an entry becomes terminal - see the class remarks. <see langword="null"/> (default) disables it, relying solely on explicit <see cref="PurgeAsync"/> calls. Only takes effect once <see cref="EnsureIndexesAsync"/> has run.</param>
        public MongoInboxStore(
            IMongoDatabase database,
            string collectionName = DefaultCollectionName,
            string metaCollectionName = DefaultMetaCollectionName,
            TimeProvider? timeProvider = null,
            TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(database);
            ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
            ArgumentException.ThrowIfNullOrWhiteSpace(metaCollectionName);

            _collection = database.GetCollection<InboxMessageDocument>(collectionName);
            _metaCollection = database.GetCollection<SchemaVersionDocument>(metaCollectionName);
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

        /// <summary>
        /// Ensures the collection's indexes exist (via <see cref="EnsureIndexesAsync"/>, itself idempotent
        /// and safe under concurrent replicas - MongoDB's <c>createIndexes</c> converges without error for
        /// concurrent, identically-specified calls) and reconciles the persisted schema-version document
        /// at <c>{metaCollectionName}/schema</c> against <see cref="RequiredSchemaVersion"/>.
        /// </summary>
        /// <remarks>
        /// No distributed lock is needed here (contrast the EF Core backend's advisory lock, or the Redis
        /// backend's <c>SET ... NX</c> mutual exclusion): the version reconciliation is itself a
        /// compare-and-set loop - <see cref="StoreInitializationMode.Apply"/>'s "first ever start" case
        /// uses an atomic <c>$setOnInsert</c> upsert (only one concurrent caller's insert actually takes
        /// effect; the rest observe the document already existing) and its "needs upgrading" case
        /// CAS-guards the update on the exact old version it read, retrying (bounded by
        /// <see cref="MaxCasAttempts"/>) if another replica already moved it - the same pattern
        /// <see cref="TryClaimAsync"/> itself uses for a claim. There is no document/index-layout change
        /// to actually migrate yet (version 1 is this backend's first-ever schema) - an upgrade today is
        /// only ever the version document's own creation or bump; a future version bump that does change
        /// the persisted document/index shape would apply its migration steps here, before writing the
        /// new version.
        /// </remarks>
        public async Task<StoreInitializationResult> InitializeAsync(StoreInitializationMode mode = StoreInitializationMode.Apply, CancellationToken cancellationToken = default)
        {
            if (mode == StoreInitializationMode.VerifyOnly)
                return await VerifySchemaAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                await EnsureIndexesAsync(cancellationToken).ConfigureAwait(false);

                var upsert = await _metaCollection.UpdateOneAsync(
                    Builders<SchemaVersionDocument>.Filter.Eq(x => x.Id, SchemaDocId),
                    Builders<SchemaVersionDocument>.Update.SetOnInsert(x => x.Version, RequiredSchemaVersion),
                    new UpdateOptions { IsUpsert = true },
                    cancellationToken).ConfigureAwait(false);

                if (upsert.UpsertedId is not null)
                    return Result(StoreInitializationOutcome.Upgraded, null, $"MongoDB schema initialized at version {RequiredSchemaVersion}.");

                for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
                {
                    var doc = await _metaCollection.Find(x => x.Id == SchemaDocId).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                    if (doc is null)
                        continue; // Deleted between the upsert above and this read - vanishingly rare; retry from the top.

                    if (doc.Version == RequiredSchemaVersion)
                        return Result(StoreInitializationOutcome.Ready, doc.Version, "MongoDB schema already at the required version.");

                    if (doc.Version > RequiredSchemaVersion)
                        return Result(StoreInitializationOutcome.IncompatibleVersion, doc.Version,
                            $"Persisted MongoDB schema version {doc.Version} is newer than this code's version {RequiredSchemaVersion} - this code is too old to talk to this database safely.");

                    var filter = Builders<SchemaVersionDocument>.Filter.Eq(x => x.Id, SchemaDocId) & Builders<SchemaVersionDocument>.Filter.Eq(x => x.Version, doc.Version);
                    var update = Builders<SchemaVersionDocument>.Update.Set(x => x.Version, RequiredSchemaVersion);
                    var result = await _metaCollection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
                    if (result.MatchedCount == 1)
                        return Result(StoreInitializationOutcome.Upgraded, doc.Version, $"MongoDB schema upgraded from version {doc.Version} to {RequiredSchemaVersion}.");
                    // Else: another replica already changed it since our read - retry the whole cycle.
                }

                throw new InvalidOperationException(
                    $"Exceeded {MaxCasAttempts} attempts reconciling the Inbox schema version - this indicates pathological concurrency, not a normal outcome.");
            }
            catch (MongoException exception)
            {
                return Result(StoreInitializationOutcome.InsufficientPermissions, null, "MongoDB rejected a read/write needed to initialize the schema.", exception);
            }
        }

        private async Task<StoreInitializationResult> VerifySchemaAsync(CancellationToken cancellationToken)
        {
            try
            {
                var doc = await _metaCollection.Find(x => x.Id == SchemaDocId).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                if (doc is null)
                    return Result(StoreInitializationOutcome.IncompatibleVersion, null,
                        "No MongoDB schema-version document found - this database has never been initialized, and VerifyOnly mode never creates one.");

                return doc.Version == RequiredSchemaVersion
                    ? Result(StoreInitializationOutcome.VerifiedCompatible, doc.Version, "MongoDB schema verified compatible.")
                    : Result(StoreInitializationOutcome.IncompatibleVersion, doc.Version, $"Persisted version {doc.Version} does not match required version {RequiredSchemaVersion}.");
            }
            catch (MongoException exception)
            {
                return Result(StoreInitializationOutcome.InsufficientPermissions, null, "MongoDB rejected the read needed to verify the schema.", exception);
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
        public async Task<InboxPurgeResult> PurgeAsync(InboxPurgeRequest request, CancellationToken cancellationToken = default)
        {
            var builder = Builders<InboxMessageDocument>.Filter;
            var statusFilters = new List<FilterDefinition<InboxMessageDocument>>();

            if (request.ProcessedOlderThanUtc is { } processedCutoff)
                statusFilters.Add(builder.Eq(x => x.Status, InboxMessageStatus.Processed) & builder.Lt(x => x.TerminalAtUtc, processedCutoff.UtcDateTime));

            if (request.DeadLetteredOlderThanUtc is { } deadLetteredCutoff)
                statusFilters.Add(builder.Eq(x => x.Status, InboxMessageStatus.DeadLettered) & builder.Lt(x => x.TerminalAtUtc, deadLetteredCutoff.UtcDateTime));

            if (statusFilters.Count == 0)
                return new InboxPurgeResult { PurgedCount = 0, HasMore = false };

            var filter = builder.Eq(x => x.ChannelKey, request.ChannelKey.ToString("N")) & builder.Or(statusFilters);

            // Two-step (select bounded ID batch, then delete by ID list) rather than a single bounded
            // DeleteMany: MongoDB's deleteMany has no built-in limit/sort, so a bounded, oldest-first
            // batch requires finding the IDs first.
            var eligibleIds = await _collection.Find(filter)
                .Sort(Builders<InboxMessageDocument>.Sort.Ascending(x => x.TerminalAtUtc))
                .Limit(request.MaxCount + 1)
                .Project(x => x.Id)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);

            var hasMore = eligibleIds.Count > request.MaxCount;
            var batchIds = hasMore ? eligibleIds.Take(request.MaxCount).ToList() : eligibleIds;
            var idsToDelete = batchIds;

            if (request.ExcludedIds is { Count: > 0 })
            {
                var excludedIdStrings = request.ExcludedIds.Select(id => id.ToString("N")).ToHashSet();
                idsToDelete = [.. batchIds.Where(id => !excludedIdStrings.Contains(id))];
            }

            if (idsToDelete.Count == 0)
                return new InboxPurgeResult { PurgedCount = 0, HasMore = hasMore };

            var deleteResult = await _collection.DeleteManyAsync(builder.In(x => x.Id, idsToDelete), cancellationToken).ConfigureAwait(false);
            return new InboxPurgeResult { PurgedCount = (int)deleteResult.DeletedCount, HasMore = hasMore };
        }

        /// <inheritdoc/>
        public async Task<InboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default)
        {
            var idString = id.ToString("N");

            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var doc = await _collection.Find(x => x.Id == idString).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                if (doc is null)
                    return null;

                var existing = InboxMessageMapper.ToDomain(doc);
                if (existing.Status is not (InboxMessageStatus.Processed or InboxMessageStatus.DeadLettered))
                    return null;

                var replayed = existing.Replay(_timeProvider);
                if (await TryReplaceAsync(idString, doc.Version, replayed, cancellationToken).ConfigureAwait(false))
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
