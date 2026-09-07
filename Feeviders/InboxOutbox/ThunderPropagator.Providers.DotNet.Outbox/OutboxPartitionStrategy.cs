namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>How a Provider's outgoing messages are assigned to an <see cref="OutboxMessage.PartitionKey"/>.</summary>
    /// <remarks>
    /// Ordering is only guaranteed within one partition (see <see cref="IOutboxStore.ClaimBatchAsync"/>),
    /// so this is fundamentally a throughput/ordering trade-off: fewer, broader partitions mean stronger
    /// ordering but less claim parallelism; more, narrower partitions mean more parallelism but ordering
    /// only within each narrow slice. Independent partitions always progress concurrently and
    /// independently - a relay worker never lets one partition's backlog, failure, or backoff delay
    /// another's.
    /// </remarks>
    /// <remarks>
    /// Resolution is centralized, not per-transport: every Provider (regardless of which broker it
    /// targets) resolves its <see cref="OutboxMessage.PartitionKey"/> the same way, from this one
    /// strategy plus its own <see cref="OutboxOptions"/> - the canonical mapping is
    /// <see langword="None"/> → <see langword="null"/>, <see langword="Fixed"/> →
    /// <see cref="OutboxOptions.FixedPartitionKey"/>, <see langword="CallerSupplied"/> → whatever the
    /// enqueuing call site set on the outgoing <c>FeederMessage</c>. No transport overrides this
    /// resolution, so ordering/partitioning behavior is guaranteed identical across every supported
    /// Provider.
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
