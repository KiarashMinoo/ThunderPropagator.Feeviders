using Microsoft.Extensions.Logging;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Emits one bounded operational notification per dead-lettered entry via a caller-supplied
    /// <see cref="IInboxDeadLetterNotificationSink"/> - "bounded" because it calls the sink at most once
    /// per <see cref="HandleAsync"/> invocation, and recursion-protected because it refuses to notify
    /// about an entry that is itself an outgoing dead-letter notification (see
    /// <see cref="RecursionGuardHeader"/>): without this guard, a notification sent as a message that can
    /// itself be dead-lettered (e.g. published through another Inbox/Outbox subscription) would notify
    /// about its own delivery failures forever.
    /// </summary>
    public sealed class InboxChannelDeadLetterHandler(IInboxDeadLetterNotificationSink sink, ILogger<InboxChannelDeadLetterHandler> logger) : IInboxDeadLetterHandler
    {
        /// <summary>
        /// Header key an <see cref="IInboxDeadLetterNotificationSink"/> must stamp on any message it
        /// sends as the notification itself, if that message can re-enter this same dead-letter
        /// machinery. Its presence on <see cref="InboxDeadLetterContext.Headers"/> tells this handler to
        /// suppress notifying about it again.
        /// </summary>
        public const string RecursionGuardHeader = "x-thunderpropagator-dead-letter-notification";

        public async ValueTask<InboxDeadLetterHandlerOutcome> HandleAsync(InboxDeadLetterContext context, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (context.Headers.ContainsKey(RecursionGuardHeader))
            {
                logger.LogWarning(
                    "Suppressed a dead-letter notification for entry {Id} (message '{MessageId}') because it was itself an outgoing dead-letter notification - notifying about it again would risk notifying forever.",
                    context.Id, context.MessageId);

                return InboxDeadLetterHandlerOutcome.Skipped;
            }

            await sink.NotifyAsync(context, cancellationToken).ConfigureAwait(false);
            return InboxDeadLetterHandlerOutcome.Handled;
        }
    }
}
