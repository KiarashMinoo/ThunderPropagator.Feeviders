namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// One stage of the dead-letter pipeline (<see cref="OutboxDeadLetterPipeline"/>) invoked after an
    /// entry is durably marked <see cref="OutboxMessageStatus.DeadLettered"/> - the extension point
    /// routing, alerting, or audit-logging tooling plugs into. Registered per relay subscription (zero or
    /// more, run in registration order); a subscription with none simply skips this step, so
    /// dead-lettering itself never depends on one existing.
    /// </summary>
    public interface IOutboxDeadLetterHandler
    {
        /// <summary>
        /// Notified after the entry <paramref name="context"/> describes was durably marked DeadLettered.
        /// A thrown exception is caught by <see cref="OutboxDeadLetterPipeline"/> and isolated to this
        /// one handler's result - it never stops the durable dead-letter record from existing, and never
        /// prevents the next handler in the pipeline from running.
        /// </summary>
        ValueTask<OutboxDeadLetterHandlerOutcome> HandleAsync(OutboxDeadLetterContext context, CancellationToken cancellationToken);
    }
}
