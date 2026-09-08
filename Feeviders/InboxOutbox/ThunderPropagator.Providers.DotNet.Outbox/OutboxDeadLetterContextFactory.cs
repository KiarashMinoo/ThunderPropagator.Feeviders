using System.Diagnostics;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Builds the <see cref="OutboxDeadLetterContext"/> handed to every handler in a dead-letter pipeline.</summary>
    public static class OutboxDeadLetterContextFactory
    {
        /// <param name="message">The just-dead-lettered snapshot - <see cref="OutboxMessage.Status"/> must already be <see cref="OutboxMessageStatus.DeadLettered"/>.</param>
        /// <param name="failureCategory">See <see cref="OutboxFailureClassifier.Classify"/>.</param>
        /// <param name="payloadPolicy">See <see cref="OutboxDeadLetterPayloadPolicy"/>. Defaults to <see cref="OutboxDeadLetterPayloadPolicy.Include"/>.</param>
        public static OutboxDeadLetterContext Create(OutboxMessage message, OutboxDeadLetterFailureCategory failureCategory, OutboxDeadLetterPayloadPolicy payloadPolicy = OutboxDeadLetterPayloadPolicy.Include)
        {
            ArgumentNullException.ThrowIfNull(message);

            return new OutboxDeadLetterContext
            {
                Id = message.Id,
                MessageId = message.MessageId,
                ProviderKey = message.ProviderKey,
                PartitionKey = message.PartitionKey,
                Attempts = message.Attempts,
                CreatedAtUtc = message.CreatedAtUtc,
                DeadLetteredAtUtc = message.DeadLetteredAtUtc ?? DateTimeOffset.UtcNow,
                FailureCategory = failureCategory,
                FailureReason = message.FailureReason ?? string.Empty,
                TraceId = Activity.Current?.Id,
                Payload = payloadPolicy == OutboxDeadLetterPayloadPolicy.Include ? message.Payload : null,
                PayloadContentType = payloadPolicy != OutboxDeadLetterPayloadPolicy.Omit ? message.PayloadContentType : null,
                Headers = message.Headers,
            };
        }
    }
}
