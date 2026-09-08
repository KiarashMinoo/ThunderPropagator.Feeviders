using Microsoft.Extensions.Diagnostics.HealthChecks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ThunderPropagator.Providers.DotNet.Outbox.MongoDB
{
    /// <summary>
    /// <see cref="IOutboxStore"/> backed by <see href="https://www.mongodb.com/">MongoDB</see> via the
    /// official MongoDB.Driver. Durable across restarts as long as the MongoDB deployment itself
    /// persists. Cannot participate in <see cref="OutboxTransactionMode.Enlisted"/> (that mode is scoped
    /// to a shared-<c>DbContext</c> relational transaction - see <see cref="OutboxStoreType.EFCore"/>'s
    /// remarks) - <see cref="OutboxOptions.Validate"/> already rejects that combination for
    /// <see cref="OutboxStoreType.MongoDB"/> at configuration time. A MongoDB-native equivalent - sharing
    /// an actual MongoDB multi-document transaction between an Outbox enqueue and the caller's own
    /// business documents, on the topologies that support it (a replica set or sharded cluster; a
    /// standalone <c>mongod</c> does not) - is instead exposed directly on this store, see
    /// <see cref="EnqueueAsync(OutboxEnqueueRequest, IClientSessionHandle, CancellationToken)"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Atomicity.</b> Every field-level decision is computed in C# using <see cref="OutboxMessage"/>'s
    /// own transition rules - exactly the same rules every other backend uses. MongoDB's role is purely to
    /// make the read-decide-write sequence atomic via a single-document <c>replaceOne</c> filtered on both
    /// the document's <c>_id</c> and its <see cref="OutboxMessageDocument.Version"/> (a compare-and-swap
    /// MongoDB's own single-document write atomicity gives for free, no server-side scripting needed). A
    /// version conflict during <see cref="ClaimBatchAsync"/> simply skips that one candidate rather than
    /// retrying or failing the whole batch - another caller already claimed or otherwise mutated it, so it
    /// is no longer this call's to claim.
    /// </para>
    /// <para>
    /// <b>Ordering sequence.</b> Allocated via a single atomic <c>findOneAndUpdate</c> with <c>$inc</c>
    /// (upsert) against a dedicated <see cref="OutboxSequenceDocument"/> per partition - see that type's
    /// remarks. Unlike the EF Core backend (which needs an optimistic-retry loop for the same purpose,
    /// since a relational engine has no equivalent single-operation atomic increment portable across every
    /// supported provider), MongoDB's <c>$inc</c> already is the atomic primitive, so no retry loop is
    /// needed here.
    /// </para>
    /// <para>
    /// <b>Document/field limits.</b> See <c>ThunderPropagator.Feeders.Inbox.MongoDB.MongoInboxStore</c>'s
    /// remarks - the same reasoning applies here: this store's own payload/header ceilings already leave
    /// every document comfortably under MongoDB's 16MB single-document limit.
    /// </para>
    /// <para>
    /// <b>Retention.</b> <see cref="PurgeAsync"/> is the primary retention mechanism. <see cref="EnsureIndexesAsync"/>
    /// optionally also creates a native MongoDB TTL index on <see cref="OutboxMessageDocument.TerminalAtUtc"/> -
    /// see <c>MongoInboxStore</c>'s remarks on the equivalent Inbox-side field for why a non-terminal entry
    /// can never expire under it.
    /// </para>
    /// <para>
    /// <b>Indexes are not created automatically</b> - call <see cref="EnsureIndexesAsync"/> once at startup.
    /// </para>
    /// <para>
    /// <b>Not throughput-optimized.</b> Like the Redis backend, <see cref="ClaimBatchAsync"/> scans a
    /// partition's entire claimable set rather than a bounded window, favoring correctness/auditability
    /// over raw throughput.
    /// </para>
    /// </remarks>
    public sealed class MongoOutboxStore : IOutboxStore, IHealthCheck, IOutboxStoreInitializer
    {
        /// <summary>Default message collection name - see <see cref="MongoOutboxStoreServiceCollectionExtensions.AddMongoOutboxStore"/> for overriding it.</summary>
        public const string DefaultCollectionName = "__TP_Outbox";

        /// <summary>Default ordering-sequence-counter collection name - see <see cref="MongoOutboxStoreServiceCollectionExtensions.AddMongoOutboxStore"/> for overriding it.</summary>
        public const string DefaultSequenceCollectionName = "__TP_Outbox_Sequence";

        /// <summary>Default schema-version metadata collection name - see <see cref="MongoOutboxStoreServiceCollectionExtensions.AddMongoOutboxStore"/> for overriding it.</summary>
        public const string DefaultMetaCollectionName = "__TP_Outbox_Meta";

        /// <summary><see cref="OutboxSequenceDocument.Id"/>/partition-filter stand-in for the <see langword="null"/> partition, which cannot be a document <c>_id</c> value.</summary>
        internal const string NullPartitionId = "~~null-partition~~";

        /// <summary>
        /// This store's document-shape/index layout version - see <see cref="InitializeAsync"/>. Bump
        /// whenever a change here would make an older version of this class misread existing documents.
        /// </summary>
        private const int RequiredSchemaVersion = 1;

        private const int MaxCasAttempts = 20;
        private const string SchemaDocId = "schema";

        private readonly IMongoCollection<OutboxMessageDocument> _collection;
        private readonly IMongoCollection<OutboxSequenceDocument> _sequenceCollection;
        private readonly IMongoCollection<SchemaVersionDocument> _metaCollection;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan? _terminalEntryTtl;

        /// <param name="database">Owned by the caller - never disposed by this store.</param>
        /// <param name="collectionName">Message collection this instance reads/writes. Defaults to <see cref="DefaultCollectionName"/>.</param>
        /// <param name="sequenceCollectionName">Ordering-sequence-counter collection. Defaults to <see cref="DefaultSequenceCollectionName"/>.</param>
        /// <param name="metaCollectionName">Schema-version metadata collection - see <see cref="InitializeAsync"/>. Defaults to <see cref="DefaultMetaCollectionName"/>.</param>
        /// <param name="timeProvider">Clock used for timestamps and lease/retry expiry. Defaults to <see cref="TimeProvider.System"/>.</param>
        /// <param name="terminalEntryTtl">Passive-safety-net TTL applied once an entry becomes terminal. <see langword="null"/> (default) disables it. Only takes effect once <see cref="EnsureIndexesAsync"/> has run.</param>
        public MongoOutboxStore(
            IMongoDatabase database,
            string collectionName = DefaultCollectionName,
            string sequenceCollectionName = DefaultSequenceCollectionName,
            string metaCollectionName = DefaultMetaCollectionName,
            TimeProvider? timeProvider = null,
            TimeSpan? terminalEntryTtl = null)
        {
            ArgumentNullException.ThrowIfNull(database);
            ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
            ArgumentException.ThrowIfNullOrWhiteSpace(sequenceCollectionName);
            ArgumentException.ThrowIfNullOrWhiteSpace(metaCollectionName);

            _collection = database.GetCollection<OutboxMessageDocument>(collectionName);
            _sequenceCollection = database.GetCollection<OutboxSequenceDocument>(sequenceCollectionName);
            _metaCollection = database.GetCollection<SchemaVersionDocument>(metaCollectionName);
            _timeProvider = timeProvider ?? TimeProvider.System;
            _terminalEntryTtl = terminalEntryTtl;
        }

        /// <summary>
        /// Creates the status/retry/partition and (when a <c>terminalEntryTtl</c> was supplied) TTL
        /// indexes this store depends on. Idempotent - safe to call every time the process starts.
        /// </summary>
        public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
        {
            var models = new List<CreateIndexModel<OutboxMessageDocument>>
            {
                new(Builders<OutboxMessageDocument>.IndexKeys
                        .Ascending(x => x.PartitionKey).Ascending(x => x.Status).Ascending(x => x.OrderingSequence),
                    new CreateIndexOptions { Name = "ix_partition_status_sequence" }),
                new(Builders<OutboxMessageDocument>.IndexKeys.Ascending(x => x.Status),
                    new CreateIndexOptions { Name = "ix_status" }),
                new(Builders<OutboxMessageDocument>.IndexKeys.Ascending(x => x.MessageId),
                    new CreateIndexOptions { Name = "ix_message_id" }),
            };

            if (_terminalEntryTtl is { } ttl)
            {
                models.Add(new CreateIndexModel<OutboxMessageDocument>(
                    Builders<OutboxMessageDocument>.IndexKeys.Ascending(x => x.TerminalAtUtc),
                    new CreateIndexOptions { Name = "ttl_terminal", ExpireAfter = ttl }));
            }

            await _collection.Indexes.CreateManyAsync(models, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures the collection's indexes exist and reconciles the persisted schema-version document
        /// against <see cref="RequiredSchemaVersion"/> - see <c>MongoInboxStore.InitializeAsync</c>'s
        /// remarks for the full design (no distributed lock needed; a compare-and-set loop mirroring
        /// <see cref="ClaimBatchAsync"/>'s own CAS pattern; nothing to actually migrate yet since version
        /// 1 is this backend's first-ever schema).
        /// </summary>
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
                    $"Exceeded {MaxCasAttempts} attempts reconciling the Outbox schema version - this indicates pathological concurrency, not a normal outcome.");
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
        public Task<OutboxMessage> EnqueueAsync(OutboxEnqueueRequest request, CancellationToken cancellationToken = default) =>
            EnqueueAsync(request, session: null, cancellationToken);

        /// <summary>
        /// Enqueues <paramref name="request"/> as part of <paramref name="session"/>'s own transaction -
        /// the MongoDB-native "transactions for outbox plus business documents" capability described on
        /// the class remarks. The caller starts and commits/aborts <paramref name="session"/> itself
        /// (typically wrapping this call and its own business-document writes, all issued against that
        /// same session, in <see cref="IClientSession.WithTransactionAsync{TResult}"/> or an
        /// explicit <see cref="IClientSession.StartTransaction"/>/<see cref="IClientSession.CommitTransactionAsync"/>
        /// pair) - this method only ever adds one more operation to it, and only succeeds against a
        /// deployment topology that supports multi-document transactions (a replica set or sharded
        /// cluster). Against a standalone <c>mongod</c>, the driver itself rejects starting a transaction -
        /// callers on that topology should use the plain, explicitly non-transactional
        /// <see cref="EnqueueAsync(OutboxEnqueueRequest, CancellationToken)"/> overload instead.
        /// </summary>
        public async Task<OutboxMessage> EnqueueAsync(OutboxEnqueueRequest request, IClientSessionHandle? session, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);

            var orderingSequence = await AllocateNextOrderingSequenceAsync(request.PartitionKey, session, cancellationToken).ConfigureAwait(false);
            var message = OutboxMessage.CreatePending(
                Guid.NewGuid(), request.MessageId, request.ProviderKey, orderingSequence,
                request.SchemaVersion, request.PayloadContentType, request.Payload,
                request.Headers, request.PartitionKey, _timeProvider);

            var document = OutboxMessageMapper.ToDocument(message, version: 1);
            if (session is null)
                await _collection.InsertOneAsync(document, cancellationToken: cancellationToken).ConfigureAwait(false);
            else
                await _collection.InsertOneAsync(session, document, cancellationToken: cancellationToken).ConfigureAwait(false);

            return message;
        }

        private async Task<long> AllocateNextOrderingSequenceAsync(string? partitionKey, IClientSessionHandle? session, CancellationToken cancellationToken)
        {
            var id = partitionKey ?? NullPartitionId;
            var filter = Builders<OutboxSequenceDocument>.Filter.Eq(x => x.Id, id);
            var update = Builders<OutboxSequenceDocument>.Update.Inc(x => x.NextValue, 1L);
            var options = new FindOneAndUpdateOptions<OutboxSequenceDocument>
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After,
            };

            var updated = session is null
                ? await _sequenceCollection.FindOneAndUpdateAsync(filter, update, options, cancellationToken).ConfigureAwait(false)
                : await _sequenceCollection.FindOneAndUpdateAsync(session, filter, update, options, cancellationToken).ConfigureAwait(false);

            return updated.NextValue - 1;
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string? partitionKey, int maxCount, string leaseOwner, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            var now = _timeProvider.GetUtcNow();
            var candidates = await _collection.Find(ClaimableFilter(partitionKey, now.UtcDateTime))
                .SortBy(x => x.OrderingSequence)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);

            var claimed = new List<OutboxMessage>();
            foreach (var candidate in candidates)
            {
                if (claimed.Count >= maxCount)
                    break;

                var existing = OutboxMessageMapper.ToDomain(candidate);

                // An abandoned Publishing lease must first release back through Failed (Publishing isn't
                // a valid source for Publishing in OutboxMessage's transition table) before it can be
                // reclaimed. Every claim increments the attempt count unconditionally.
                var source = existing.Status == OutboxMessageStatus.Publishing
                    ? existing.TryTransitionTo(OutboxMessageStatus.Failed, _timeProvider)
                    : existing;
                var publishing = source.TryTransitionTo(
                    OutboxMessageStatus.Publishing, _timeProvider,
                    leaseOwner: leaseOwner, leaseExpiresAtUtc: now + leaseDuration, incrementAttempt: true);

                if (await TryReplaceAsync(candidate.Id, candidate.Version, publishing, cancellationToken).ConfigureAwait(false))
                    claimed.Add(publishing);
                // Else: another caller claimed/mutated this entry since our read above - move on.
            }

            return claimed;
        }

        private static FilterDefinition<OutboxMessageDocument> ClaimableFilter(string? partitionKey, DateTime now)
        {
            var builder = Builders<OutboxMessageDocument>.Filter;
            return builder.Eq(x => x.PartitionKey, partitionKey) & (
                builder.Eq(x => x.Status, OutboxMessageStatus.Pending)
                | (builder.Eq(x => x.Status, OutboxMessageStatus.Publishing) &
                   (builder.Eq(x => x.LeaseExpiresAtUtc, null) | builder.Lte(x => x.LeaseExpiresAtUtc, now)))
                | (builder.Eq(x => x.Status, OutboxMessageStatus.Failed) &
                   (builder.Eq(x => x.NextRetryAtUtc, null) | builder.Lte(x => x.NextRetryAtUtc, now)))
            );
        }

        private async Task<bool> TryReplaceAsync(string id, long expectedVersion, OutboxMessage updated, CancellationToken cancellationToken)
        {
            var filter = Builders<OutboxMessageDocument>.Filter.Eq(x => x.Id, id) &
                         Builders<OutboxMessageDocument>.Filter.Eq(x => x.Version, expectedVersion);
            var replacement = OutboxMessageMapper.ToDocument(updated, expectedVersion + 1);

            var result = await _collection.ReplaceOneAsync(filter, replacement, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.MatchedCount == 1;
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string?>> GetClaimablePartitionKeysAsync(CancellationToken cancellationToken = default)
        {
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            var builder = Builders<OutboxMessageDocument>.Filter;
            var filter = builder.Eq(x => x.Status, OutboxMessageStatus.Pending)
                | (builder.Eq(x => x.Status, OutboxMessageStatus.Publishing) &
                   (builder.Eq(x => x.LeaseExpiresAtUtc, null) | builder.Lte(x => x.LeaseExpiresAtUtc, now)))
                | (builder.Eq(x => x.Status, OutboxMessageStatus.Failed) &
                   (builder.Eq(x => x.NextRetryAtUtc, null) | builder.Lte(x => x.NextRetryAtUtc, now)));

            var partitionKeys = await _collection.Find(filter).Project(x => x.PartitionKey).ToListAsync(cancellationToken).ConfigureAwait(false);
            return partitionKeys.Distinct().ToArray();
        }

        /// <inheritdoc/>
        public Task<OutboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.RenewLease(_timeProvider, leaseOwner, leaseExtension), cancellationToken);

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkPublishedAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Published, _timeProvider), cancellationToken);

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkFailedAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Failed, _timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason), cancellationToken);

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkDeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            TransitionLeasedAsync(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.DeadLettered, _timeProvider, failureReason: failureReason), cancellationToken);

        private async Task<OutboxMessage?> TransitionLeasedAsync(Guid id, string leaseOwner, Func<OutboxMessage, OutboxMessage?> transition, CancellationToken cancellationToken)
        {
            var idString = id.ToString("N");
            var doc = await _collection.Find(x => x.Id == idString).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (doc is null)
                return null;

            var existing = OutboxMessageMapper.ToDomain(doc);
            if (existing.LeaseOwner != leaseOwner)
                return null;

            var updated = transition(existing);
            if (updated is null)
                return null;

            return await TryReplaceAsync(idString, doc.Version, updated, cancellationToken).ConfigureAwait(false) ? updated : null;
        }

        /// <inheritdoc/>
        public async Task<bool> ReleaseAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default)
        {
            var idString = id.ToString("N");
            var doc = await _collection.Find(x => x.Id == idString).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (doc is null || doc.Status != OutboxMessageStatus.Publishing || doc.LeaseOwner != leaseOwner)
                return false;

            var filter = Builders<OutboxMessageDocument>.Filter.Eq(x => x.Id, idString) &
                         Builders<OutboxMessageDocument>.Filter.Eq(x => x.Version, doc.Version);
            var update = Builders<OutboxMessageDocument>.Update
                .Set(x => x.Status, OutboxMessageStatus.Pending)
                .Unset(x => x.LeaseOwner)
                .Unset(x => x.LeaseExpiresAtUtc)
                .Unset(x => x.PublishingStartedAtUtc)
                .Inc(x => x.Version, 1L);

            var result = await _collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.MatchedCount == 1;
        }

        /// <inheritdoc/>
        public async Task<int> GetDepthAsync(string? partitionKey, CancellationToken cancellationToken = default)
        {
            var builder = Builders<OutboxMessageDocument>.Filter;
            var filter = builder.Nin(x => x.Status, [OutboxMessageStatus.Published, OutboxMessageStatus.DeadLettered]);
            if (partitionKey is not null)
                filter &= builder.Eq(x => x.PartitionKey, partitionKey);

            return (int)await _collection.CountDocumentsAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<TimeSpan?> GetOldestPendingAgeAsync(string? partitionKey, TimeProvider ageTimeProvider, CancellationToken cancellationToken = default)
        {
            var builder = Builders<OutboxMessageDocument>.Filter;
            var filter = builder.Nin(x => x.Status, [OutboxMessageStatus.Published, OutboxMessageStatus.DeadLettered]);
            if (partitionKey is not null)
                filter &= builder.Eq(x => x.PartitionKey, partitionKey);

            var oldest = await _collection.Find(filter)
                .SortBy(x => x.CreatedAtUtc)
                .Limit(1)
                .FirstOrDefaultAsync(cancellationToken)
                .ConfigureAwait(false);

            return oldest is null ? null : ageTimeProvider.GetUtcNow() - new DateTimeOffset(DateTime.SpecifyKind(oldest.CreatedAtUtc, DateTimeKind.Utc));
        }

        /// <inheritdoc/>
        public async Task<int> PurgeAsync(DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default)
        {
            var builder = Builders<OutboxMessageDocument>.Filter;
            var filter = builder.In(x => x.Status, [OutboxMessageStatus.Published, OutboxMessageStatus.DeadLettered]) &
                         builder.Lt(x => x.TerminalAtUtc, olderThanUtc.UtcDateTime);

            var result = await _collection.DeleteManyAsync(filter, cancellationToken).ConfigureAwait(false);
            return (int)result.DeletedCount;
        }

        /// <inheritdoc/>
        public async Task<OutboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default)
        {
            var idString = id.ToString("N");

            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var doc = await _collection.Find(x => x.Id == idString).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                if (doc is null)
                    return null;

                var existing = OutboxMessageMapper.ToDomain(doc);
                if (existing.Status is not (OutboxMessageStatus.Published or OutboxMessageStatus.DeadLettered))
                    return null;

                var newOrderingSequence = await AllocateNextOrderingSequenceAsync(existing.PartitionKey, session: null, cancellationToken).ConfigureAwait(false);
                var requeued = existing.Requeue(newOrderingSequence, _timeProvider);

                if (await TryReplaceAsync(idString, doc.Version, requeued, cancellationToken).ConfigureAwait(false))
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
