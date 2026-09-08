namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Runs every registered <see cref="IInboxDeadLetterHandler"/> for one dead-lettered entry, in
    /// registration order, isolating each handler's own failure from every other - a routing handler
    /// throwing must never prevent an alerting handler (registered after it) from still running, and
    /// neither ever risks the durable dead-letter record itself, which is already persisted before this
    /// pipeline runs.
    /// </summary>
    /// <remarks>
    /// Cancellation is honored between handlers, not mid-handler: once the token passed to
    /// <see cref="RunAsync"/> is signaled, no further handler starts, but a handler already
    /// running is given the chance to observe cancellation itself via the same token forwarded to
    /// <see cref="IInboxDeadLetterHandler.HandleAsync"/>. A handler not yet run when cancellation is
    /// observed is simply absent from <see cref="InboxDeadLetterPipelineResult.HandlerResults"/> - not
    /// recorded as a failure, since it never got a chance to succeed or fail.
    /// </remarks>
    public sealed class InboxDeadLetterPipeline(IReadOnlyList<IInboxDeadLetterHandler> handlers)
    {
        /// <summary>Runs every handler for <paramref name="context"/>, in order.</summary>
        public async Task<InboxDeadLetterPipelineResult> RunAsync(InboxDeadLetterContext context, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(context);

            var results = new List<InboxDeadLetterHandlerResult>(handlers.Count);

            foreach (var handler in handlers)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                try
                {
                    var outcome = await handler.HandleAsync(context, cancellationToken).ConfigureAwait(false);
                    results.Add(new InboxDeadLetterHandlerResult { HandlerType = handler.GetType(), Outcome = outcome });
                }
                catch (Exception exception)
                {
                    results.Add(new InboxDeadLetterHandlerResult { HandlerType = handler.GetType(), Error = exception });
                }
            }

            return new InboxDeadLetterPipelineResult { HandlerResults = results };
        }
    }
}
