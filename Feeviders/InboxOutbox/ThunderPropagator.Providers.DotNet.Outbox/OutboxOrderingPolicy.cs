namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// What a relay worker does with the rest of a claimed batch when one entry in a partition fails
    /// transiently (not dead-lettered) - see <see cref="OutboxOptions.OrderingPolicy"/>.
    /// </summary>
    public enum OutboxOrderingPolicy
    {
        /// <summary>
        /// Default. A transient failure releases every later entry already claimed in the same
        /// partition/batch back to <see cref="OutboxMessageStatus.Pending"/> instead of publishing them -
        /// publishing them now would put them ahead of the failed entry once it is retried, which this
        /// policy never allows. The safe default for any Provider where downstream consumers assume
        /// strict per-partition ordering.
        /// </summary>
        StrictPerPartition,

        /// <summary>
        /// A transient failure does not block the rest of the batch - later entries in the same
        /// partition publish immediately, ahead of the failed entry (which is retried independently on
        /// its own backoff schedule). Trades strict per-partition ordering for availability/throughput:
        /// only use this where downstream consumers do not depend on ordering, or already reorder/dedupe
        /// (see <see cref="OutboxOptions"/> remarks on downstream idempotency) - and where a poison entry
        /// stuck retrying must never stall everything behind it in its partition.
        /// </summary>
        ContinueOnFailure,
    }
}
