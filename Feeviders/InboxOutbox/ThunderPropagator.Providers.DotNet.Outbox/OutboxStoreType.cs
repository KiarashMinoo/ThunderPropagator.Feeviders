namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Which <see cref="IOutboxStore"/> backend a Provider's Outbox is persisted to.</summary>
    public enum OutboxStoreType
    {
        /// <summary>
        /// Process-local, non-durable. Fastest and requires no <see cref="OutboxOptions.StoreConnectionName"/>,
        /// but every entry is lost on restart or crash - never satisfies <see cref="OutboxOptions.RequireDurableStore"/>
        /// and cannot participate in <see cref="OutboxTransactionMode.Enlisted"/>.
        /// </summary>
        InMemory,

        /// <summary>
        /// Backed by Redis via <see cref="OutboxOptions.StoreConnectionName"/>. Durable across restarts as
        /// long as the Redis deployment itself persists (RDB/AOF). Cannot participate in
        /// <see cref="OutboxTransactionMode.Enlisted"/> - Redis has no shared local transaction with an
        /// arbitrary business-side relational store.
        /// </summary>
        Redis,

        /// <summary>
        /// Backed by a relational database via EF Core and <see cref="OutboxOptions.StoreConnectionName"/>.
        /// Durable, and the only backend that can support <see cref="OutboxTransactionMode.Enlisted"/> -
        /// where the same <c>DbContext</c>/connection also owns business state, the outbox row commits in
        /// the same local transaction as that state.
        /// </summary>
        EFCore,

        /// <summary>
        /// Backed by MongoDB via <see cref="OutboxOptions.StoreConnectionName"/>. Durable across restarts;
        /// cannot participate in <see cref="OutboxTransactionMode.Enlisted"/> - a MongoDB session/transaction
        /// is not shared with an arbitrary business-side relational store.
        /// </summary>
        MongoDB,
    }
}
