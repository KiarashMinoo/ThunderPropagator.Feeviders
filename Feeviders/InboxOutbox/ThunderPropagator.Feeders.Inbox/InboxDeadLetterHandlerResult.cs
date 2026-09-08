namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>What happened when <see cref="InboxDeadLetterPipeline"/> ran one <see cref="IInboxDeadLetterHandler"/>.</summary>
    public sealed record InboxDeadLetterHandlerResult
    {
        /// <summary>The concrete handler type this result is for - handlers are not required to be uniquely named otherwise.</summary>
        public required Type HandlerType { get; init; }

        /// <summary>The handler's own reported outcome, or <see langword="null"/> if it threw instead - see <see cref="Error"/>.</summary>
        public InboxDeadLetterHandlerOutcome? Outcome { get; init; }

        /// <summary>The exception the handler threw, if any. Never rethrown by the pipeline - see <see cref="InboxDeadLetterPipeline"/>'s remarks.</summary>
        public Exception? Error { get; init; }

        /// <summary>Whether this handler completed without throwing.</summary>
        public bool Succeeded => Error is null;
    }
}
