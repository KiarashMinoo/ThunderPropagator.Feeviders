namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Which <see cref="IInboxStore"/> backend a Feevider's Inbox is persisted to.</summary>
    public enum InboxStoreType
    {
        /// <summary>
        /// Process-local, non-durable. Fastest and requires no <see cref="InboxOptions.StoreConnectionName"/>,
        /// but every entry is lost on restart or crash - never satisfies <see cref="InboxOptions.RequireDurableStore"/>
        /// and does not provide the durable at-least-once guarantee documented on <see cref="InboxOptions"/>.
        /// </summary>
        InMemory,

        /// <summary>
        /// Backed by Redis via <see cref="InboxOptions.StoreConnectionName"/>. Durable across restarts as
        /// long as the Redis deployment itself persists (RDB/AOF) - a cache-only Redis instance without
        /// persistence enabled does not provide the durable guarantee <see cref="InboxOptions.RequireDurableStore"/> asserts.
        /// </summary>
        Redis,

        /// <summary>
        /// Backed by a relational database via EF Core and <see cref="InboxOptions.StoreConnectionName"/>.
        /// Durable and, where the same <c>DbContext</c>/connection also owns business state, able to
        /// commit the Inbox claim in the same local transaction as that state.
        /// </summary>
        EFCore,

        /// <summary>
        /// Backed by MongoDB via <see cref="InboxOptions.StoreConnectionName"/>. Durable across restarts;
        /// atomic claims rely on single-document compare-and-swap rather than a shared local transaction
        /// with business state.
        /// </summary>
        MongoDB,
    }
}
