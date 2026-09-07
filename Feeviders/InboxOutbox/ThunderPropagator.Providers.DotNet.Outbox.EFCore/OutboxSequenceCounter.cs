namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Backs <see cref="EfCoreOutboxStore"/>'s atomic, per-partition <see cref="OutboxMessage.OrderingSequence"/>
    /// allocation - one row per partition (a null <see cref="OutboxMessage.PartitionKey"/> is tracked
    /// under <see cref="EfCoreOutboxStore.NullPartitionSentinel"/>, since a primary key column cannot be
    /// null), holding the next value to assign. An implementation detail of <see cref="EfCoreOutboxStore"/>,
    /// never surfaced through <see cref="IOutboxStore"/> - but since there is no owning <c>DbContext</c>
    /// here (see <see cref="OutboxMessageEntityTypeConfiguration"/>'s remarks), a caller must apply
    /// <see cref="OutboxSequenceCounterEntityTypeConfiguration"/> to their own model alongside the main
    /// one for <see cref="EfCoreOutboxStore.EnqueueAsync"/> to work at all.
    /// </summary>
    public sealed class OutboxSequenceCounter
    {
        public required string PartitionKey { get; init; }

        public long NextValue { get; set; }
    }
}
