namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Whether <see cref="IOutboxStore.EnqueueAsync"/> shares a local transaction with business state.</summary>
    public enum OutboxTransactionMode
    {
        /// <summary>
        /// <see cref="IOutboxStore.EnqueueAsync"/> is not enlisted in the same local transaction as
        /// business state - a durable buffer a relay worker eventually drains, not a true Transactional
        /// Outbox. Business state can commit while the outbox row does not (or vice versa) if the
        /// process crashes between the two writes. The safe default: it assumes no particular unit-of-work
        /// support from either side.
        /// </summary>
        NonTransactional,

        /// <summary>
        /// <see cref="IOutboxStore.EnqueueAsync"/> commits business state and the outbox row in one
        /// supported local transaction - both durably persist together, or neither does. Requires a
        /// <see cref="OutboxOptions.StoreType"/> capable of sharing that transaction with the business
        /// store (today, only <see cref="OutboxStoreType.EFCore"/> - see <see cref="OutboxOptions.Validate"/>).
        /// The actual enlistment boundary (which unit of work, which <c>DbContext</c>) is defined by the
        /// caller, not by this option.
        /// </summary>
        Enlisted,
    }
}
