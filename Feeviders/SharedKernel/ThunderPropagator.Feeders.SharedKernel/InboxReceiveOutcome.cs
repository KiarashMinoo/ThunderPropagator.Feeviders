namespace ThunderPropagator.Feeders.SharedKernel
{
    /// <summary>Result of one <see cref="InboxReceiveCoordinator.ReceiveAsync"/> call.</summary>
    public enum InboxReceiveOutcome
    {
        /// <summary>Inbox is disabled for this Feevider - the handler ran directly, no store was touched.</summary>
        Disabled,

        /// <summary>Claimed, the handler ran successfully, and the entry was marked Processed.</summary>
        Processed,

        /// <summary>Claimed, the handler threw, and the entry was marked Failed (retry-eligible).</summary>
        Failed,

        /// <summary>Claimed, the handler threw, retries were exhausted, and the entry was marked DeadLettered.</summary>
        DeadLettered,

        /// <summary>A message with this dedup key already reached Processed - the handler did not run.</summary>
        Duplicate,

        /// <summary>Another worker currently holds an unexpired lease on this dedup key - the handler did not run.</summary>
        InProgress,

        /// <summary>A message with this dedup key is already DeadLettered - the handler did not run.</summary>
        AlreadyDeadLettered,
    }
}
