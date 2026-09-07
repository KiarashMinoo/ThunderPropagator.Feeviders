using Microsoft.EntityFrameworkCore;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// The <see cref="OutboxTransactionMode.Enlisted"/> <see cref="IOutboxUnitOfWork"/>: stages Outbox
    /// rows as tracked entities on a caller-supplied <see cref="DbContext"/> (which must apply
    /// <see cref="OutboxMessageEntityTypeConfiguration"/> to its model) and commits them via that same
    /// context's own <see cref="DbContext.SaveChangesAsync(CancellationToken)"/>. Because business
    /// entities tracked on the same <paramref name="dbContext"/> instance are written in that identical
    /// <c>SaveChangesAsync</c> call, both persist in one relational transaction, or - if that call throws
    /// - neither does. This type never calls <see cref="DbContext.SaveChangesAsync(CancellationToken)"/>
    /// on any schedule of its own and never disposes <paramref name="dbContext"/> - the caller owns both;
    /// this only adds/removes tracked Outbox entities on it.
    /// </summary>
    /// <remarks>
    /// <see cref="OutboxMessage.OrderingSequence"/> is assigned client-side, starting at zero for each
    /// <see cref="EfCoreOutboxUnitOfWork"/> instance - correct only for ordering the messages staged
    /// within THIS instance relative to each other, not across separate transactions or instances
    /// racing the same partition. A production-grade backend needs a database-level mechanism (a
    /// sequence, an identity column, or an equivalent) to assign a partition-scoped sequence free of
    /// that race; providing one is a real backend's concern (see the Storage Backends epic), not this
    /// enlistment boundary's.
    /// </remarks>
    public sealed class EfCoreOutboxUnitOfWork(DbContext dbContext, TimeProvider? timeProvider = null) : IOutboxUnitOfWork
    {
        private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
        private readonly List<OutboxMessage> _staged = [];
        private long _nextOrderingSequence;

        /// <inheritdoc/>
        public void Enqueue(OutboxEnqueueRequest request)
        {
            ArgumentNullException.ThrowIfNull(request);

            var message = OutboxMessage.CreatePending(
                Guid.NewGuid(), request.MessageId, request.ProviderKey, _nextOrderingSequence++,
                request.SchemaVersion, request.PayloadContentType, request.Payload,
                request.Headers, request.PartitionKey, _timeProvider);

            _staged.Add(message);
            dbContext.Add(message);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<OutboxMessage>> CommitAsync(CancellationToken cancellationToken = default)
        {
            if (_staged.Count == 0)
                return [];

            // Deliberately the caller's own DbContext, not a private one - whatever business entities
            // are also tracked on it right now are written in this exact same call, and therefore this
            // exact same relational transaction.
            await dbContext.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

            var committed = _staged.ToArray();
            _staged.Clear();
            return committed;
        }

        /// <summary>
        /// Detaches every staged-but-never-committed entity from the <see cref="DbContext"/>'s change
        /// tracker - without this, an uncommitted stage would otherwise still be tracked as "Added" and
        /// could be written by surprise the next time the caller calls <c>SaveChangesAsync</c> for an
        /// unrelated reason. Never disposes the <see cref="DbContext"/> itself - the caller owns it.
        /// </summary>
        public ValueTask DisposeAsync()
        {
            foreach (var message in _staged)
                dbContext.Entry(message).State = EntityState.Detached;

            _staged.Clear();
            return ValueTask.CompletedTask;
        }
    }
}
