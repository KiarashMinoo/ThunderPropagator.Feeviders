namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Outcome of <see cref="IInboxStore.TryClaimAsync"/>. <see cref="Message"/> is populated for
    /// <see cref="InboxClaimOutcome.Claimed"/> (the caller's own new lease) and, where the backend
    /// can cheaply provide it, for the other outcomes too (the existing entry that blocked the claim).
    /// </summary>
    public sealed record InboxClaimResult(InboxClaimOutcome Outcome, InboxMessage? Message)
    {
        public static InboxClaimResult Claimed(InboxMessage message) => new(InboxClaimOutcome.Claimed, message);

        public static InboxClaimResult AlreadyProcessed(InboxMessage? message = null) => new(InboxClaimOutcome.AlreadyProcessed, message);

        public static InboxClaimResult ClaimedByAnotherOwner(InboxMessage? message = null) => new(InboxClaimOutcome.ClaimedByAnotherOwner, message);

        public static InboxClaimResult DeadLettered(InboxMessage? message = null) => new(InboxClaimOutcome.DeadLettered, message);
    }
}
