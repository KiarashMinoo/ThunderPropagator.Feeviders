using Microsoft.EntityFrameworkCore;
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
    public sealed class EfCoreInboxStore : IInboxStore, IHealthCheck
    {
        /// <summary>
        /// See <see cref="InboxMessageEntityTypeConfiguration.DedupPartitionKeyPropertyName"/>'s remarks.
        /// Deliberately contains no NUL byte - unlike every in-memory/Redis sentinel elsewhere in this
        /// codebase, PostgreSQL's <c>text</c>/<c>varchar</c> columns reject a NUL byte outright
        /// (<c>22021: invalid byte sequence for encoding "UTF8": 0x00</c>) rather than merely storing it.
        /// </summary>
        internal const string NullPartitionSentinel = "~~null-partition~~";

        private const int MaxConcurrencyRetries = 20;

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
        public async Task<int> PurgeAsync(Guid channelKey, DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default)
        {
            await using var db = _createDbContext();

            return await db.Set<InboxMessage>()
                .Where(m => m.ChannelKey == channelKey)
                .Where(m =>
                    (m.Status == InboxMessageStatus.Processed && m.ProcessedAtUtc < olderThanUtc) ||
                    (m.Status == InboxMessageStatus.DeadLettered && m.DeadLetteredAtUtc < olderThanUtc))
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
