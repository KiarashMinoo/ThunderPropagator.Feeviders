using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// <see cref="IInboxStore"/> backed by a relational database via EF Core. Portable across every
    /// relational provider EF Core supports - correctness here never depends on a provider-specific
    /// locking clause (contrast <c>EfCoreOutboxStore.ClaimBatchAsync</c>, which does, for the batch
    /// SKIP LOCKED scan a single-row Inbox claim has no equivalent need for).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Atomicity.</b> Every field-level decision is computed in C# using <see cref="InboxMessage"/>'s
    /// own transition rules - exactly the same rules every other backend (InMemory, Redis) uses. A fresh
    /// claim races the database's own unique index on (ChannelKey, DedupPartitionKey, MessageId) - see
    /// <see cref="InboxMessageEntityTypeConfiguration"/> - via an INSERT that simply fails (caught and
    /// treated as "someone else already has this dedup key, go read it") if another caller won the race.
    /// A reclaim (an existing row's lease expired, or its retry delay elapsed) is protected purely by
    /// optimistic concurrency: <see cref="InboxMessageEntityTypeConfiguration.VersionPropertyName"/>, a
    /// portable application-managed shadow property, not a native <c>rowversion</c>/<c>xmin</c> column.
    /// A concurrency conflict retries the whole read-decide-write cycle (bounded by
    /// <see cref="MaxConcurrencyRetries"/>), since the entry's state - and therefore the correct
    /// outcome - may have changed while retrying.
    /// </para>
    /// <para>
    /// <b>Migrations.</b> See <see cref="InboxMessageEntityTypeConfiguration"/>'s remarks - standard EF
    /// Core migration lifecycle on whatever <c>DbContext</c> the caller applies that configuration to.
    /// </para>
    /// </remarks>
    public sealed class EfCoreInboxStore : IInboxStore, IHealthCheck, IInboxStoreInitializer
    {
        /// <summary>
        /// See <see cref="InboxMessageEntityTypeConfiguration.DedupPartitionKeyPropertyName"/>'s remarks.
        /// Deliberately contains no NUL byte - unlike every in-memory/Redis sentinel elsewhere in this
        /// codebase, PostgreSQL's <c>text</c>/<c>varchar</c> columns reject a NUL byte outright
        /// (<c>22021: invalid byte sequence for encoding "UTF8": 0x00</c>) rather than merely storing it.
        /// </summary>
        internal const string NullPartitionSentinel = "~~null-partition~~";

        private const int MaxConcurrencyRetries = 20;

        /// <summary>
        /// This store's table/index layout version - see <see cref="InitializeAsync"/>. Bump whenever a
        /// change here would make an older version of this class misread an existing schema.
        /// </summary>
        private const int RequiredSchemaVersion = 1;

        private const string SchemaRowId = "schema";
        private static readonly TimeSpan SchemaLockTimeout = TimeSpan.FromSeconds(30);

        private readonly Func<DbContext> _createDbContext;
        private readonly TimeProvider _timeProvider;

        /// <param name="createDbContext">
        /// Returns a new, unshared <see cref="DbContext"/> instance on every call - this store opens and
        /// disposes one per operation, since a <see cref="DbContext"/> is not safe for concurrent use.
        /// The returned context's model must have applied <see cref="InboxMessageEntityTypeConfiguration"/>.
        /// </param>
        /// <param name="timeProvider">Clock used for timestamps and lease/retry expiry. Defaults to <see cref="TimeProvider.System"/>.</param>
        public EfCoreInboxStore(Func<DbContext> createDbContext, TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(createDbContext);
            _createDbContext = createDbContext;
            _timeProvider = timeProvider ?? TimeProvider.System;
        }

        /// <summary>
        /// Creates this database's Inbox tables (if none exist yet) or upgrades the persisted schema
        /// version (if older than <see cref="RequiredSchemaVersion"/>), tracked in
        /// <see cref="InboxSchemaVersion"/> - the caller's model must also apply
        /// <see cref="InboxSchemaVersionEntityTypeConfiguration"/> for this to work at all.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>Concurrent replicas.</b> <see cref="StoreInitializationMode.Apply"/> holds a provider
        /// advisory lock (<see cref="SchemaLockDialect"/>) for the entire read-decide-write sequence, on
        /// one connection kept open for that duration - this is the one operation in this store that
        /// genuinely needs a real lock rather than optimistic concurrency, since the very first replica
        /// to run <c>CREATE TABLE</c> against a not-yet-existing schema has nothing to optimistically
        /// retry against yet. <see cref="StoreInitializationMode.VerifyOnly"/> never mutates anything and
        /// therefore never needs the lock.
        /// </para>
        /// <para>
        /// <b>"Fails clearly" is a deliberate simplification.</b> Any <see cref="DbException"/> raised
        /// while creating tables, taking the lock, or reading/writing the version row is reported as
        /// <see cref="StoreInitializationOutcome.InsufficientPermissions"/> with the raw exception
        /// attached, rather than trying to distinguish "table already exists"/"permission denied"/"table
        /// not found" from each provider's own distinct exception shape - a real distinction production
        /// tooling would want, but not one this code can reliably make across three different ADO
        /// providers without dedicated per-provider error-code inspection this repo has not built. Either
        /// way the caller gets a clearly-labeled outcome and the real exception to inspect, never a raw,
        /// uncategorized failure.
        /// </para>
        /// </remarks>
        public async Task<StoreInitializationResult> InitializeAsync(StoreInitializationMode mode = StoreInitializationMode.Apply, CancellationToken cancellationToken = default)
        {
            if (mode == StoreInitializationMode.VerifyOnly)
                return await VerifySchemaAsync(cancellationToken).ConfigureAwait(false);

            await using var db = _createDbContext();
            var lockName = BuildLockName(db);

            try
            {
                await db.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

                var acquired = await SchemaLockDialect.TryAcquireAsync(db, lockName, SchemaLockTimeout, cancellationToken).ConfigureAwait(false);
                if (!acquired)
                    throw new TimeoutException(
                        $"Could not acquire the Inbox schema initialization lock '{lockName}' within {SchemaLockTimeout.TotalSeconds:F0}s - another replica appears to be stuck holding it.");

                try
                {
                    return await ApplySchemaAsync(db, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    await SchemaLockDialect.ReleaseAsync(db, lockName, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (DbException exception)
            {
                return Result(StoreInitializationOutcome.InsufficientPermissions, null, "The database rejected a read/write needed to initialize the schema.", exception);
            }
            finally
            {
                await db.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }

        private async Task<StoreInitializationResult> ApplySchemaAsync(DbContext db, CancellationToken cancellationToken)
        {
            // Deliberately not IRelationalDatabaseCreator.HasTablesAsync(): confirmed empirically that
            // it answers "does this DATABASE have any tables at all", not "does this model's own
            // schema/tables exist" - wrong wherever multiple schemas share one database (every caller
            // of this store in this repo's own tests). CreateTablesAsync's own success/failure is what's
            // actually schema/table-name-specific, so that is the real "already initialized?" signal.
            var creator = db.GetService<IRelationalDatabaseCreator>();
            try
            {
                await creator.CreateTablesAsync(cancellationToken).ConfigureAwait(false);
                db.Add(new InboxSchemaVersion { Id = SchemaRowId, Version = RequiredSchemaVersion });
                await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                return Result(StoreInitializationOutcome.Upgraded, null, $"Inbox schema created at version {RequiredSchemaVersion}.");
            }
            catch (DbException)
            {
                // Tables already exist (this replica's own earlier call, or a concurrent one under the
                // same lock never runs concurrently, so realistically: a previous call entirely) - fall
                // through to read-and-reconcile the persisted version below. A genuine permission problem
                // surfaces from the read/write that follows instead - see the class remarks.
            }

            var row = await db.Set<InboxSchemaVersion>().FirstOrDefaultAsync(x => x.Id == SchemaRowId, cancellationToken).ConfigureAwait(false);
            if (row is null)
            {
                // Tables already exist but no version row does - a schema created before this
                // initializer existed. Version 1 is this backend's only schema shape so far, so simply
                // recording it now is correct; a future version bump would need real migration steps here.
                db.Add(new InboxSchemaVersion { Id = SchemaRowId, Version = RequiredSchemaVersion });
                await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                return Result(StoreInitializationOutcome.Upgraded, null, $"Schema-version row created at version {RequiredSchemaVersion} for a pre-existing schema.");
            }

            if (row.Version == RequiredSchemaVersion)
                return Result(StoreInitializationOutcome.Ready, row.Version, "Inbox schema already at the required version.");

            if (row.Version > RequiredSchemaVersion)
                return Result(StoreInitializationOutcome.IncompatibleVersion, row.Version,
                    $"Persisted Inbox schema version {row.Version} is newer than this code's version {RequiredSchemaVersion} - this code is too old to talk to this database safely.");

            var previousVersion = row.Version;
            row.Version = RequiredSchemaVersion;
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            return Result(StoreInitializationOutcome.Upgraded, previousVersion, $"Inbox schema upgraded from version {previousVersion} to {RequiredSchemaVersion}.");
        }

        private async Task<StoreInitializationResult> VerifySchemaAsync(CancellationToken cancellationToken)
        {
            await using var db = _createDbContext();

            try
            {
                var row = await db.Set<InboxSchemaVersion>().FirstOrDefaultAsync(x => x.Id == SchemaRowId, cancellationToken).ConfigureAwait(false);
                if (row is null)
                    return Result(StoreInitializationOutcome.IncompatibleVersion, null, "No schema-version row was found (tables may not exist at all).");

                return row.Version == RequiredSchemaVersion
                    ? Result(StoreInitializationOutcome.VerifiedCompatible, row.Version, "Inbox schema verified compatible.")
                    : Result(StoreInitializationOutcome.IncompatibleVersion, row.Version, $"Persisted version {row.Version} does not match required version {RequiredSchemaVersion}.");
            }
            catch (DbException exception)
            {
                // Most likely the schema-version table itself does not exist yet - i.e. this schema was
                // never initialized, which VerifyOnly must report as "not compatible", not as a
                // permission problem: a real permission failure would already have surfaced from every
                // other read this store performs, not uniquely from this one.
                return Result(StoreInitializationOutcome.IncompatibleVersion, null, "Could not read the schema-version row - this schema was likely never initialized.", exception);
            }
        }

        private static string BuildLockName(DbContext db)
        {
            var entityType = db.Model.FindEntityType(typeof(InboxMessage));
            var table = entityType?.GetTableName() ?? nameof(InboxMessage);
            var schema = entityType?.GetSchema();
            return schema is null ? $"ThunderPropagator.Inbox.Schema.{table}" : $"ThunderPropagator.Inbox.Schema.{schema}.{table}";
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

            for (var attempt = 0; attempt < MaxConcurrencyRetries; attempt++)
            {
                await using var db = _createDbContext();
                var existing = await FindAsync(db, request.ChannelKey, request.PartitionKey, request.MessageId, cancellationToken).ConfigureAwait(false);
                if (existing is null)
                {
                    // The row that won the insert race was purged before we could read it back -
                    // vanishingly rare, but the correct response is simply to try inserting again.
                    if (await TryInsertAsync(request, cancellationToken).ConfigureAwait(false) is { } recreated)
                        return InboxClaimResult.Claimed(recreated);
                    continue;
                }

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

                if (await TryApplyAsync(db, existing, reclaimed, cancellationToken).ConfigureAwait(false))
                    return InboxClaimResult.Claimed(reclaimed);
            }

            throw new InvalidOperationException(
                $"Exceeded {MaxConcurrencyRetries} attempts contending for an Inbox claim on message '{request.MessageId}' - this indicates pathological concurrency, not a normal outcome.");
        }

        private async Task<InboxMessage?> TryInsertAsync(InboxClaimRequest request, CancellationToken cancellationToken)
        {
            await using var db = _createDbContext();
            var now = _timeProvider.GetUtcNow();
            var received = InboxMessage.CreateReceived(
                Guid.NewGuid(), request.MessageId, request.ChannelKey, request.FeederId, request.SchemaVersion,
                request.PayloadContentType, request.Payload, request.Headers, request.PartitionKey, _timeProvider);
            var claimed = received.TryTransitionTo(
                InboxMessageStatus.Processing, _timeProvider,
                leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

            var entry = db.Add(claimed);
            entry.Property(InboxMessageEntityTypeConfiguration.DedupPartitionKeyPropertyName).CurrentValue = request.PartitionKey ?? NullPartitionSentinel;
            try
            {
                await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                return claimed;
            }
            catch (DbUpdateException)
            {
                // Almost certainly the unique (ChannelKey, DedupPartitionKey, MessageId) index rejecting a
                // duplicate - another caller inserted this dedup key first. Nothing to roll back: this
                // context's SaveChangesAsync already failed atomically. The caller falls back to reading
                // the now-existing row.
                return null;
            }
        }

        private static Task<InboxMessage?> FindAsync(DbContext db, Guid channelKey, string? partitionKey, string messageId, CancellationToken cancellationToken) =>
            db.Set<InboxMessage>()
                .FirstOrDefaultAsync(m => m.ChannelKey == channelKey && m.PartitionKey == partitionKey && m.MessageId == messageId, cancellationToken);

        /// <summary>Applies <paramref name="updated"/> onto the tracked <paramref name="existing"/> entry and saves, under the shadow Version concurrency check. Returns <see langword="false"/> on a lost race.</summary>
        private static async Task<bool> TryApplyAsync(DbContext db, InboxMessage existing, InboxMessage updated, CancellationToken cancellationToken)
        {
            var entry = db.Entry(existing);
            entry.CurrentValues.SetValues(updated);
            var versionProperty = entry.Property<long>(InboxMessageEntityTypeConfiguration.VersionPropertyName);
            versionProperty.CurrentValue = versionProperty.CurrentValue + 1;

            try
            {
                await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (DbUpdateConcurrencyException)
            {
                return false;
            }
        }

        /// <inheritdoc/>
        public async Task<InboxMessage?> GetAsync(string messageId, Guid channelKey, string? partitionKey, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            return await FindAsync(db, channelKey, partitionKey, messageId, cancellationToken).ConfigureAwait(false);
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
            await using var db = _createDbContext();
            var existing = await db.Set<InboxMessage>().FirstOrDefaultAsync(m => m.Id == id, cancellationToken).ConfigureAwait(false);
            if (existing is null || existing.LeaseOwner != leaseOwner)
                return null;

            var updated = transition(existing);
            if (updated is null)
                return null;

            return await TryApplyAsync(db, existing, updated, cancellationToken).ConfigureAwait(false) ? updated : null;
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            var now = _timeProvider.GetUtcNow();

            return await db.Set<InboxMessage>()
                .Where(m => m.ChannelKey == channelKey)
                .Where(m =>
                    (m.Status == InboxMessageStatus.Failed && (m.NextRetryAtUtc == null || m.NextRetryAtUtc <= now)) ||
                    (m.Status == InboxMessageStatus.Processing && (m.LeaseExpiresAtUtc == null || m.LeaseExpiresAtUtc <= now)))
                .Take(maxCount)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<InboxPurgeResult> PurgeAsync(InboxPurgeRequest request, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();

            // Two-step (select bounded ID batch, then delete by ID list) rather than a single
            // ExecuteDeleteAsync composed with Take: Take-before-ExecuteDelete translation support is
            // provider-dependent, but every relational provider here can translate a plain ORDER BY +
            // Select(Id) + Take, and an ExecuteDeleteAsync filtered by an ID list translates everywhere.
            var eligibleIds = await db.Set<InboxMessage>()
                .Where(m => m.ChannelKey == request.ChannelKey)
                .Where(m =>
                    (request.ProcessedOlderThanUtc != null && m.Status == InboxMessageStatus.Processed && m.ProcessedAtUtc < request.ProcessedOlderThanUtc) ||
                    (request.DeadLetteredOlderThanUtc != null && m.Status == InboxMessageStatus.DeadLettered && m.DeadLetteredAtUtc < request.DeadLetteredOlderThanUtc))
                .OrderBy(m => m.Status == InboxMessageStatus.Processed ? m.ProcessedAtUtc : m.DeadLetteredAtUtc)
                .Select(m => m.Id)
                .Take(request.MaxCount + 1)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);

            var hasMore = eligibleIds.Count > request.MaxCount;
            var batchIds = hasMore ? eligibleIds.Take(request.MaxCount).ToList() : eligibleIds;
            var idsToDelete = request.ExcludedIds is { Count: > 0 }
                ? batchIds.Where(id => !request.ExcludedIds.Contains(id)).ToList()
                : batchIds;

            if (idsToDelete.Count == 0)
                return new InboxPurgeResult { PurgedCount = 0, HasMore = hasMore };

            var purgedCount = await db.Set<InboxMessage>()
                .Where(m => idsToDelete.Contains(m.Id))
                .ExecuteDeleteAsync(cancellationToken)
                .ConfigureAwait(false);

            return new InboxPurgeResult { PurgedCount = purgedCount, HasMore = hasMore };
        }

        /// <inheritdoc/>
        public async Task<InboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            var existing = await db.Set<InboxMessage>().FirstOrDefaultAsync(m => m.Id == id, cancellationToken).ConfigureAwait(false);
            if (existing is null || existing.Status is not (InboxMessageStatus.Processed or InboxMessageStatus.DeadLettered))
                return null;

            var replayed = existing.Replay(_timeProvider);
            return await TryApplyAsync(db, existing, replayed, cancellationToken).ConfigureAwait(false) ? replayed : null;
        }

        /// <inheritdoc/>
        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                await using var db = _createDbContext();
                return await db.Database.CanConnectAsync(cancellationToken).ConfigureAwait(false)
                    ? HealthCheckResult.Healthy("Database connection succeeded.")
                    : HealthCheckResult.Unhealthy("Database connection failed.");
            }
            catch (Exception exception)
            {
                return HealthCheckResult.Unhealthy("Database connection threw an exception.", exception);
            }
        }
    }
}
