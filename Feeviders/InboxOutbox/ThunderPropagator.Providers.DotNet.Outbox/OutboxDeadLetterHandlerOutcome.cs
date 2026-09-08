namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>What one <see cref="IOutboxDeadLetterHandler"/> did with a <see cref="OutboxDeadLetterContext"/> it was handed.</summary>
    public enum OutboxDeadLetterHandlerOutcome
    {
        /// <summary>The handler acted on this entry (routed it, alerted on it, recorded it, etc.).</summary>
        Handled,

        /// <summary>The handler deliberately took no action (e.g. the entry did not match its own filter/routing rule) - not a failure.</summary>
        Skipped,
    }
}
