namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Coordinates enqueuing one or more Outbox messages within the boundary of one business
    /// transaction/unit of work, where the configured backend supports it - see
    /// <see cref="OutboxOptions.TransactionMode"/>. This is the enlistment boundary itself; it defines
    /// no store operations of its own beyond enqueuing (claiming/publishing remains
    /// <see cref="IOutboxStore"/>'s and a relay worker's concern).
    /// </summary>
    /// <remarks>
    /// A message staged via <see cref="Enqueue"/> is not durably persisted, and therefore never visible
    /// to a relay worker's <see cref="IOutboxStore.ClaimBatchAsync"/>, until <see cref="CommitAsync"/>
    /// returns successfully. Disposing without committing must leave nothing behind - the same "no
    /// partial state" guarantee a database transaction gives when it is rolled back instead of committed.
    /// </remarks>
    public interface IOutboxUnitOfWork : IAsyncDisposable
    {
        /// <summary>
        /// Stages <paramref name="request"/> to enqueue when <see cref="CommitAsync"/> succeeds. May be
        /// called more than once to stage several messages under one unit of work.
        /// </summary>
        void Enqueue(OutboxEnqueueRequest request);

        /// <summary>
        /// Commits every staged message. Where <see cref="OutboxOptions.TransactionMode"/> is
        /// <see cref="OutboxTransactionMode.Enlisted"/>, this commits together with whatever business
        /// state shares this unit of work's underlying transaction - both durably persist, or neither
        /// does. Where <see cref="OutboxTransactionMode.NonTransactional"/>, staged messages are
        /// persisted independently of any business state and independently of each other - a failure
        /// partway through may leave some staged messages enqueued and others not, and gives no
        /// atomicity guarantee against business state that may have already committed or may commit
        /// afterward. Returns the durably enqueued messages, in staging order.
        /// </summary>
        Task<IReadOnlyList<OutboxMessage>> CommitAsync(CancellationToken cancellationToken = default);
    }
}
