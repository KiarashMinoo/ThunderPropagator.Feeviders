using Microsoft.Extensions.Logging;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// The safe default <see cref="IInboxDeadLetterHandler"/> - logs stable identifiers and the
    /// already-sanitized failure reason at <see cref="LogLevel.Warning"/>. Never reads
    /// <see cref="InboxDeadLetterContext.Payload"/>, so it is safe to register regardless of
    /// <see cref="InboxDeadLetterPayloadPolicy"/> and requires no other configuration.
    /// </summary>
    public sealed class InboxLogDeadLetterHandler(ILogger<InboxLogDeadLetterHandler> logger) : IInboxDeadLetterHandler
    {
        public ValueTask<InboxDeadLetterHandlerOutcome> HandleAsync(InboxDeadLetterContext context, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(context);

            logger.LogWarning(
                "Inbox entry {Id} (message '{MessageId}', channel {ChannelKey}) was dead-lettered after {AttemptCount} attempt(s): {FailureCategory} - {FailureReason} (trace {TraceId}).",
                context.Id, context.MessageId, context.ChannelKey, context.AttemptCount, context.FailureCategory, context.FailureReason, context.TraceId);

            return ValueTask.FromResult(InboxDeadLetterHandlerOutcome.Handled);
        }
    }
}
