namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Aggregate outcome of running every registered <see cref="IOutboxDeadLetterHandler"/> for one dead-lettered entry.</summary>
    public sealed record OutboxDeadLetterPipelineResult
    {
        /// <summary>One result per handler, in the order they were registered/run.</summary>
        public required IReadOnlyList<OutboxDeadLetterHandlerResult> HandlerResults { get; init; }

        /// <summary>Whether every handler completed without throwing.</summary>
        public bool AllSucceeded => HandlerResults.All(r => r.Succeeded);
    }
}
