using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// <see cref="IOutboxStore"/> backed by a relational database via EF Core - SQL Server, PostgreSQL,
    /// and MySQL (via Oracle's official <c>MySql.EntityFrameworkCore</c> provider) are supported today,
    /// detected from the caller's own <c>UseSqlServer</c>/<c>UseNpgsql</c>/<c>UseMySQL</c> configuration
    /// (see <see cref="OutboxClaimSqlDialectFactory"/>); this package references none of those provider
    /// packages itself.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Atomicity.</b> Every field-level decision is computed in C# using <see cref="OutboxMessage"/>'s
    /// own transition rules - exactly the same rules every other backend (InMemory, Redis) uses.
    /// <see cref="ClaimBatchAsync"/> is the one operation that needs genuine pessimistic locking rather
    /// than optimistic concurrency: it opens an explicit transaction, uses a provider-specific
    /// <c>SELECT ... FOR UPDATE SKIP LOCKED</c> (PostgreSQL/MySQL) or <c>WITH (UPDLOCK, ROWLOCK, READPAST)</c>
    /// (SQL Server) query - see <see cref="IOutboxClaimSqlDialect"/> - to lock and skip past whatever a
    /// concurrent claim already holds, updates the locked rows through EF Core's normal change tracking,
    /// and commits. Every other lease-scoped mutation (<see cref="MarkPublishedAsync"/>,
    /// <see cref="MarkFailedAsync"/>, <see cref="MarkDeadLetterAsync"/>, <see cref="RenewLeaseAsync"/>,
    /// <see cref="ReleaseAsync"/>) instead uses plain optimistic concurrency via
    /// <see cref="OutboxMessageEntityTypeConfiguration.VersionPropertyName"/>, a portable
    /// application-managed shadow property - not a native <c>rowversion</c>/<c>xmin</c> column, which
    /// would need per-provider handling to behave identically.
    /// </para>
    /// <para>
    /// <b>Ordering.</b> <see cref="EnqueueAsync"/> allocates <see cref="OutboxMessage.OrderingSequence"/>
    /// from <see cref="OutboxSequenceCounter"/>, one row per partition, via the same portable
    /// optimistic-retry pattern (no provider-specific SQL needed here - unlike the claim path, ordinary
    /// contention on one partition's own counter is expected to be low). This closes the gap
    /// <c>EfCoreOutboxUnitOfWork</c>'s own remarks flag: a client-side-only counter is correct only
    /// within one of its instances, never across separate enqueuers racing the same partition.
    /// </para>
    /// <para>
    /// <b>Migrations.</b> See <see cref="OutboxMessageEntityTypeConfiguration"/>'s remarks - standard EF
    /// Core migration lifecycle on whatever <c>DbContext</c> the caller applies that configuration (and
    /// <see cref="OutboxSequenceCounterEntityTypeConfiguration"/>) to.
    /// </para>
    /// </remarks>
    public sealed class EfCoreOutboxStore : IOutboxStore, IHealthCheck
    {
        /// <summary>
        /// <see cref="OutboxMessage.PartitionKey"/> is nullable; <see cref="OutboxSequenceCounter.PartitionKey"/>
        /// is a primary key and therefore cannot be, so the null partition is tracked under this sentinel -
        /// a string no real caller-supplied partition key can ever equal. Deliberately contains no NUL
        /// byte - unlike every in-memory/Redis sentinel elsewhere in this codebase, PostgreSQL's
        /// <c>text</c>/<c>varchar</c> columns reject a NUL byte outright rather than merely storing it.
        /// </summary>
        internal const string NullPartitionSentinel = "~~null-partition~~";

        private const int MaxConcurrencyRetries = 20;

        private readonly Func<DbContext> _createDbContext;
        private readonly TimeProvider _timeProvider;

        /// <param name="createDbContext">
        /// Returns a new, unshared <see cref="DbContext"/> instance on every call - this store opens and
        /// disposes one per operation, since a <see cref="DbContext"/> is not safe for concurrent use.
        /// The returned context's model must have applied <see cref="OutboxMessageEntityTypeConfiguration"/>
        /// and <see cref="OutboxSequenceCounterEntityTypeConfiguration"/>.
        /// </param>
        /// <param name="timeProvider">Clock used for timestamps and lease/retry expiry. Defaults to <see cref="TimeProvider.System"/>.</param>
        public EfCoreOutboxStore(Func<DbContext> createDbContext, TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(createDbContext);
            _createDbContext = createDbContext;
            _timeProvider = timeProvider ?? TimeProvider.System;
        }

        private static string PartitionSlot(string? partitionKey) => partitionKey ?? NullPartitionSentinel;

        /// <inheritdoc/>
        public async Task<OutboxMessage> EnqueueAsync(OutboxEnqueueRequest request, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);
            await using var db = _createDbContext();
            var partitionSlot = PartitionSlot(request.PartitionKey);

            var orderingSequence = await AllocateNextOrderingSequenceAsync(db, partitionSlot, cancellationToken).ConfigureAwait(false);

            var message = OutboxMessage.CreatePending(
                Guid.NewGuid(), request.MessageId, request.ProviderKey, orderingSequence,
                request.SchemaVersion, request.PayloadContentType, request.Payload,
                request.Headers, request.PartitionKey, _timeProvider);

            db.Add(message);
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            return message;
        }

        private static async Task<long> AllocateNextOrderingSequenceAsync(DbContext db, string partitionSlot, CancellationToken cancellationToken)
        {
            for (var attempt = 0; attempt < MaxConcurrencyRetries; attempt++)
            {
                var counter = await db.Set<OutboxSequenceCounter>().FirstOrDefaultAsync(c => c.PartitionKey == partitionSlot, cancellationToken).ConfigureAwait(false);
                if (counter is null)
                {
                    var created = new OutboxSequenceCounter { PartitionKey = partitionSlot, NextValue = 1 };
                    db.Add(created);
                    try
                    {
                        await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                        return 0;
                    }
                    catch (DbUpdateException)
                    {
                        // Another enqueuer created this partition's counter first - detach and retry as
                        // the update path below.
                        db.Entry(created).State = EntityState.Detached;
                        continue;
                    }
                }

                var assigned = counter.NextValue;
                var entry = db.Entry(counter);
                var versionProperty = entry.Property<long>(OutboxSequenceCounterEntityTypeConfiguration.VersionPropertyName);
                versionProperty.CurrentValue = versionProperty.CurrentValue + 1;
                counter.NextValue = assigned + 1;

                try
                {
                    await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                    return assigned;
                }
                catch (DbUpdateConcurrencyException)
                {
                    entry.State = EntityState.Detached;
                }
            }

            throw new InvalidOperationException(
                $"Exceeded {MaxConcurrencyRetries} attempts allocating the next OrderingSequence for partition '{partitionSlot}' - this indicates pathological concurrency, not a normal outcome.");
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string? partitionKey, int maxCount, string leaseOwner, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            var dialect = OutboxClaimSqlDialectFactory.Resolve(db);
            var quotedTable = OutboxClaimSqlDialectFactory.GetQuotedTableName(db);
            var sql = dialect.BuildClaimCandidateIdsSql(quotedTable);

            var ids = await db.Database
                .SqlQueryRaw<Guid>(sql, (object?)partitionKey ?? DBNull.Value, _timeProvider.GetUtcNow(), maxCount)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);

            if (ids.Count == 0)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return [];
            }

            // The rows behind these ids are already locked (by the query above) within this same
            // transaction/connection, so this plain read sees them without contending with itself. Order
            // is not guaranteed to survive an IN-list query, so it is restored from the original,
            // already-OrderingSequence-sorted id list.
            var byId = await db.Set<OutboxMessage>().Where(m => ids.Contains(m.Id)).ToDictionaryAsync(m => m.Id, cancellationToken).ConfigureAwait(false);

            var now = _timeProvider.GetUtcNow();
            var claimed = new List<OutboxMessage>(ids.Count);
            foreach (var id in ids)
            {
                if (!byId.TryGetValue(id, out var existing))
                    continue; // Deleted (e.g. purged) between the lock and this read - vanishingly rare.

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

                ApplyChanges(db, existing, publishing);
                claimed.Add(publishing);
            }

            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return claimed;
        }

        /// <summary>Copies <paramref name="updated"/>'s field values onto the tracked <paramref name="existing"/> entry and bumps the shadow Version - does not itself save.</summary>
        private static void ApplyChanges(DbContext db, OutboxMessage existing, OutboxMessage updated)
        {
            var entry = db.Entry(existing);
            entry.CurrentValues.SetValues(updated);
            var versionProperty = entry.Property<long>(OutboxMessageEntityTypeConfiguration.VersionPropertyName);
            versionProperty.CurrentValue = versionProperty.CurrentValue + 1;
        }

        /// <summary>Applies <paramref name="updated"/> and saves under the shadow Version concurrency check. Returns <see langword="false"/> on a lost race.</summary>
        private static async Task<bool> TryApplyAsync(DbContext db, OutboxMessage existing, OutboxMessage updated, CancellationToken cancellationToken)
        {
            ApplyChanges(db, existing, updated);
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
        public async Task<IReadOnlyList<string?>> GetClaimablePartitionKeysAsync(CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            var now = _timeProvider.GetUtcNow();

            return await db.Set<OutboxMessage>()
                .Where(m =>
                    m.Status == OutboxMessageStatus.Pending ||
                    (m.Status == OutboxMessageStatus.Publishing && (m.LeaseExpiresAtUtc == null || m.LeaseExpiresAtUtc <= now)) ||
                    (m.Status == OutboxMessageStatus.Failed && (m.NextRetryAtUtc == null || m.NextRetryAtUtc <= now)))
                .Select(m => m.PartitionKey)
                .Distinct()
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
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
            await using var db = _createDbContext();
            var existing = await db.Set<OutboxMessage>().FirstOrDefaultAsync(m => m.Id == id, cancellationToken).ConfigureAwait(false);
            if (existing is null || existing.LeaseOwner != leaseOwner)
                return null;

            var updated = transition(existing);
            if (updated is null)
                return null;

            return await TryApplyAsync(db, existing, updated, cancellationToken).ConfigureAwait(false) ? updated : null;
        }

        /// <inheritdoc/>
        public async Task<bool> ReleaseAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            var existing = await db.Set<OutboxMessage>().FirstOrDefaultAsync(m => m.Id == id, cancellationToken).ConfigureAwait(false);
            if (existing is null || existing.LeaseOwner != leaseOwner || existing.Status != OutboxMessageStatus.Publishing)
                return false;

            // Released, not failed - preserves OrderingSequence and does not record a failure, so it
            // goes straight back to Pending rather than through the normal transition table (which has
            // no Publishing -> Pending edge).
            var released = existing with { Status = OutboxMessageStatus.Pending, LeaseOwner = null, LeaseExpiresAtUtc = null, PublishingStartedAtUtc = null };
            return await TryApplyAsync(db, existing, released, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<int> GetDepthAsync(string? partitionKey, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            var query = db.Set<OutboxMessage>().Where(m => m.Status != OutboxMessageStatus.Published && m.Status != OutboxMessageStatus.DeadLettered);
            if (partitionKey is not null)
                query = query.Where(m => m.PartitionKey == partitionKey);

            return await query.CountAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<TimeSpan?> GetOldestPendingAgeAsync(string? partitionKey, TimeProvider ageTimeProvider, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();
            var query = db.Set<OutboxMessage>().Where(m => m.Status != OutboxMessageStatus.Published && m.Status != OutboxMessageStatus.DeadLettered);
            if (partitionKey is not null)
                query = query.Where(m => m.PartitionKey == partitionKey);

            var oldest = await query.OrderBy(m => m.CreatedAtUtc).Select(m => (DateTimeOffset?)m.CreatedAtUtc).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            return oldest is null ? null : ageTimeProvider.GetUtcNow() - oldest.Value;
        }

        /// <inheritdoc/>
        public async Task<int> PurgeAsync(DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();

            return await db.Set<OutboxMessage>()
                .Where(m =>
                    (m.Status == OutboxMessageStatus.Published && m.PublishedAtUtc < olderThanUtc) ||
                    (m.Status == OutboxMessageStatus.DeadLettered && m.DeadLetteredAtUtc < olderThanUtc))
                .ExecuteDeleteAsync(cancellationToken)
                .ConfigureAwait(false);
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
