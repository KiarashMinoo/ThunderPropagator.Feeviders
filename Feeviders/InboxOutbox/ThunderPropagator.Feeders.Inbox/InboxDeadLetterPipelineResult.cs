namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Aggregate outcome of running every registered <see cref="IInboxDeadLetterHandler"/> for one dead-lettered entry.</summary>
    public sealed record InboxDeadLetterPipelineResult
    {
        /// <summary>One result per handler, in the order they were registered/run.</summary>
        public required IReadOnlyList<InboxDeadLetterHandlerResult> HandlerResults { get; init; }

        /// <summary>Whether every handler completed without throwing.</summary>
        public bool AllSucceeded => HandlerResults.All(r => r.Succeeded);
    }
}
