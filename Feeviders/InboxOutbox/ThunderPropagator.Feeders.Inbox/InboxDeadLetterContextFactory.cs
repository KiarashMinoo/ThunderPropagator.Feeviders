using System.Diagnostics;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Builds the <see cref="InboxDeadLetterContext"/> handed to every handler in a dead-letter pipeline.</summary>
    public static class InboxDeadLetterContextFactory
    {
        /// <param name="message">The just-dead-lettered snapshot - <see cref="InboxMessage.Status"/> must already be <see cref="InboxMessageStatus.DeadLettered"/>.</param>
        /// <param name="failureCategory">See <see cref="InboxFailureClassifier.Classify"/>.</param>
        /// <param name="payloadPolicy">See <see cref="InboxDeadLetterPayloadPolicy"/>. Defaults to <see cref="InboxDeadLetterPayloadPolicy.Include"/>.</param>
        public static InboxDeadLetterContext Create(InboxMessage message, InboxDeadLetterFailureCategory failureCategory, InboxDeadLetterPayloadPolicy payloadPolicy = InboxDeadLetterPayloadPolicy.Include)
        {
            ArgumentNullException.ThrowIfNull(message);

            return new InboxDeadLetterContext
            {
                Id = message.Id,
                MessageId = message.MessageId,
                ChannelKey = message.ChannelKey,
                FeederId = message.FeederId,
                PartitionKey = message.PartitionKey,
                AttemptCount = message.AttemptCount,
                ReceivedAtUtc = message.ReceivedAtUtc,
                DeadLetteredAtUtc = message.DeadLetteredAtUtc ?? DateTimeOffset.UtcNow,
                FailureCategory = failureCategory,
                FailureReason = message.FailureReason ?? string.Empty,
                TraceId = Activity.Current?.Id,
                Payload = payloadPolicy == InboxDeadLetterPayloadPolicy.Include ? message.Payload : null,
                PayloadContentType = payloadPolicy != InboxDeadLetterPayloadPolicy.Omit ? message.PayloadContentType : null,
                Headers = message.Headers,
            };
        }
    }
}
