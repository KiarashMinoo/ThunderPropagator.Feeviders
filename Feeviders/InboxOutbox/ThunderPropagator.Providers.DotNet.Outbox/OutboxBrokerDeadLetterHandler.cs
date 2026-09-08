namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Forwards a dead-lettered entry to a broker DLQ via a caller-supplied
    /// <see cref="IOutboxDeadLetterBrokerPublisher"/> - stamping a stable idempotency
    /// <see cref="MessageIdHeader"/> plus the original message's metadata alongside the failure
    /// classification, and retrying the publish itself a bounded number of times before giving up
    /// (letting <see cref="OutboxDeadLetterPipeline"/> isolate the final failure like it does for any
    /// other handler). Requires <see cref="OutboxDeadLetterPayloadPolicy.Include"/> - there is nothing to
    /// forward otherwise.
    /// </summary>
    public sealed class OutboxBrokerDeadLetterHandler(
        IOutboxDeadLetterBrokerPublisher publisher,
        int maxPublishAttempts = 3,
        TimeSpan? retryDelay = null,
        Func<byte[], byte[]>? payloadTransform = null) : IOutboxDeadLetterHandler
    {
        /// <summary>Stable per-message idempotency key - the same value every time this entry is (re)forwarded, so a DLQ consumer can dedupe.</summary>
        public const string MessageIdHeader = "x-thunderpropagator-dlq-message-id";

        public const string FailureCategoryHeader = "x-thunderpropagator-dlq-failure-category";
        public const string FailureReasonHeader = "x-thunderpropagator-dlq-failure-reason";
        public const string TargetHeader = "x-thunderpropagator-dlq-target";
        public const string AttemptCountHeader = "x-thunderpropagator-dlq-attempt-count";
        public const string DeadLetteredAtUtcHeader = "x-thunderpropagator-dlq-dead-lettered-at-utc";
        public const string TraceIdHeader = "x-thunderpropagator-dlq-trace-id";

        private readonly int _maxPublishAttempts = maxPublishAttempts > 0
            ? maxPublishAttempts
            : throw new ArgumentOutOfRangeException(nameof(maxPublishAttempts), maxPublishAttempts, "Must be at least 1.");

        private readonly TimeSpan _retryDelay = retryDelay ?? TimeSpan.FromSeconds(1);

        public async ValueTask<OutboxDeadLetterHandlerOutcome> HandleAsync(OutboxDeadLetterContext context, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (context.Payload is null)
                throw new InvalidOperationException(
                    $"{nameof(OutboxBrokerDeadLetterHandler)} requires {nameof(OutboxDeadLetterPayloadPolicy)}.{nameof(OutboxDeadLetterPayloadPolicy.Include)} - entry '{context.Id}' was captured with its payload redacted or omitted.");

            var payload = payloadTransform is null ? context.Payload : payloadTransform(context.Payload);

            var headers = new Dictionary<string, string>(context.Headers)
            {
                [MessageIdHeader] = context.MessageId,
                [FailureCategoryHeader] = context.FailureCategory.ToString(),
                [FailureReasonHeader] = context.FailureReason,
                [TargetHeader] = context.Target,
                [AttemptCountHeader] = context.Attempts.ToString(),
                [DeadLetteredAtUtcHeader] = context.DeadLetteredAtUtc.ToString("O"),
            };

            if (context.TraceId is not null)
                headers[TraceIdHeader] = context.TraceId;

            Exception? lastFailure = null;

            for (var attempt = 1; attempt <= _maxPublishAttempts; attempt++)
            {
                try
                {
                    await publisher.PublishAsync(context, payload, headers, cancellationToken).ConfigureAwait(false);
                    return OutboxDeadLetterHandlerOutcome.Handled;
                }
                catch (Exception exception)
                {
                    lastFailure = exception;
                    if (attempt < _maxPublishAttempts)
                        await Task.Delay(_retryDelay, cancellationToken).ConfigureAwait(false);
                }
            }

            throw new OutboxDeadLetterBrokerPublishException(
                $"Failed to publish dead-lettered entry '{context.Id}' to the broker DLQ after {_maxPublishAttempts} attempt(s).", lastFailure!);
        }
    }
}
