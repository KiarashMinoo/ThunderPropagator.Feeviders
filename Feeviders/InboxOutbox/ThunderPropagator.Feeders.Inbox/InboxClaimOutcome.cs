namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Result of an atomic <see cref="IInboxStore.TryClaimAsync"/> call.</summary>
    public enum InboxClaimOutcome
    {
        /// <summary>The caller now holds the processing lease described by the returned <see cref="InboxMessage"/>.</summary>
        Claimed,

        /// <summary>A message with this dedup key already reached <see cref="InboxMessageStatus.Processed"/>. Nothing to do - discard as a duplicate.</summary>
        AlreadyProcessed,

        /// <summary>Another worker currently holds an unexpired lease on this dedup key.</summary>
        ClaimedByAnotherOwner,

        /// <summary>A message with this dedup key is <see cref="InboxMessageStatus.DeadLettered"/> and not eligible for a new claim.</summary>
        DeadLettered,
    }
}
