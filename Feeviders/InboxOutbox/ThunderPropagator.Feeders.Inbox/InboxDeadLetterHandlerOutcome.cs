namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>What one <see cref="IInboxDeadLetterHandler"/> did with a <see cref="InboxDeadLetterContext"/> it was handed.</summary>
    public enum InboxDeadLetterHandlerOutcome
    {
        /// <summary>The handler acted on this entry (routed it, alerted on it, recorded it, etc.).</summary>
        Handled,

        /// <summary>The handler deliberately took no action (e.g. the entry did not match its own filter/routing rule) - not a failure.</summary>
        Skipped,
    }
}
