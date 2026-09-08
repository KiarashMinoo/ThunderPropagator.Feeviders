namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Runs every registered <see cref="IOutboxDeadLetterHandler"/> for one dead-lettered entry, in
    /// registration order, isolating each handler's own failure from every other - see
    /// <c>InboxDeadLetterPipeline</c>'s remarks for the identical Inbox-side design and cancellation
    /// semantics (honored between handlers, not mid-handler).
    /// </summary>
    public sealed class OutboxDeadLetterPipeline(IReadOnlyList<IOutboxDeadLetterHandler> handlers)
    {
        /// <summary>Runs every handler for <paramref name="context"/>, in order.</summary>
        public async Task<OutboxDeadLetterPipelineResult> RunAsync(OutboxDeadLetterContext context, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(context);

            var results = new List<OutboxDeadLetterHandlerResult>(handlers.Count);

            foreach (var handler in handlers)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                try
                {
                    var outcome = await handler.HandleAsync(context, cancellationToken).ConfigureAwait(false);
                    results.Add(new OutboxDeadLetterHandlerResult { HandlerType = handler.GetType(), Outcome = outcome });
                }
                catch (Exception exception)
                {
                    results.Add(new OutboxDeadLetterHandlerResult { HandlerType = handler.GetType(), Error = exception });
                }
            }

            return new OutboxDeadLetterPipelineResult { HandlerResults = results };
        }
    }
}
