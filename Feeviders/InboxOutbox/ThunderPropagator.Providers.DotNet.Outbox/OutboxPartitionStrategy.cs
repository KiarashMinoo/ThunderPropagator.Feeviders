namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>How a Provider's outgoing messages are assigned to an <see cref="OutboxMessage.PartitionKey"/>.</summary>
    /// <remarks>
    /// Ordering is only guaranteed within one partition (see <see cref="IOutboxStore.ClaimBatchAsync"/>),
    /// so this is fundamentally a throughput/ordering trade-off: fewer, broader partitions mean stronger
    /// ordering but less claim parallelism; more, narrower partitions mean more parallelism but ordering
    /// only within each narrow slice.
    /// </remarks>
    public enum OutboxPartitionStrategy
    {
        /// <summary>Every message from this Provider shares the null partition - strict FIFO across everything the Provider enqueues, no claim parallelism.</summary>
        None,

        /// <summary>Every message shares one fixed, named partition (see <see cref="OutboxOptions.FixedPartitionKey"/>) - the same strict FIFO as <see cref="None"/>, under an explicit named lane instead of null (useful for per-partition observability even when there is only one).</summary>
        Fixed,

        /// <summary>The enqueuing call site supplies its own <see cref="OutboxEnqueueRequest.PartitionKey"/> per message (e.g. an aggregate/business key) - ordering is scoped to whatever value the caller chose.</summary>
        CallerSupplied,
    }
}
