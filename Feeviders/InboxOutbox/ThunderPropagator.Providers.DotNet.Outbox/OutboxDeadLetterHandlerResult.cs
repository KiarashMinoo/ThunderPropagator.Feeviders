namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>What happened when <see cref="OutboxDeadLetterPipeline"/> ran one <see cref="IOutboxDeadLetterHandler"/>.</summary>
    public sealed record OutboxDeadLetterHandlerResult
    {
        /// <summary>The concrete handler type this result is for - handlers are not required to be uniquely named otherwise.</summary>
        public required Type HandlerType { get; init; }

        /// <summary>The handler's own reported outcome, or <see langword="null"/> if it threw instead - see <see cref="Error"/>.</summary>
        public OutboxDeadLetterHandlerOutcome? Outcome { get; init; }

        /// <summary>The exception the handler threw, if any. Never rethrown by the pipeline - see <see cref="OutboxDeadLetterPipeline"/>'s remarks.</summary>
        public Exception? Error { get; init; }

        /// <summary>Whether this handler completed without throwing.</summary>
        public bool Succeeded => Error is null;
    }
}
