using Microsoft.Extensions.Logging;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// The safe default <see cref="IOutboxDeadLetterHandler"/> - logs stable identifiers and the
    /// already-sanitized failure reason at <see cref="LogLevel.Warning"/>. Never reads
    /// <see cref="OutboxDeadLetterContext.Payload"/>, so it is safe to register regardless of
    /// <see cref="OutboxDeadLetterPayloadPolicy"/> and requires no other configuration.
    /// </summary>
    public sealed class OutboxLogDeadLetterHandler(ILogger<OutboxLogDeadLetterHandler> logger) : IOutboxDeadLetterHandler
    {
        public ValueTask<OutboxDeadLetterHandlerOutcome> HandleAsync(OutboxDeadLetterContext context, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(context);

            logger.LogWarning(
                "Outbox entry {Id} (message '{MessageId}', provider {ProviderKey}) was dead-lettered after {Attempts} attempt(s): {FailureCategory} - {FailureReason} (trace {TraceId}).",
                context.Id, context.MessageId, context.ProviderKey, context.Attempts, context.FailureCategory, context.FailureReason, context.TraceId);

            return ValueTask.FromResult(OutboxDeadLetterHandlerOutcome.Handled);
        }
    }
}
