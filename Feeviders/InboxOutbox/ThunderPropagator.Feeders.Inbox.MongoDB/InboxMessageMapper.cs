namespace ThunderPropagator.Feeders.Inbox.MongoDB
{
    /// <summary>Converts between <see cref="InboxMessage"/> and its persisted <see cref="InboxMessageDocument"/> shape.</summary>
    internal static class InboxMessageMapper
    {
        public static InboxMessageDocument ToDocument(InboxMessage message, long version) => new()
        {
            Id = message.Id.ToString("N"),
            MessageId = message.MessageId,
            ChannelKey = message.ChannelKey.ToString("N"),
            FeederId = message.FeederId.ToString("N"),
            PartitionKey = message.PartitionKey,
            SchemaVersion = message.SchemaVersion,
            PayloadContentType = message.PayloadContentType,
            Payload = message.Payload,
            Headers = new Dictionary<string, string>(message.Headers),
            Status = message.Status,
            AttemptCount = message.AttemptCount,
            ReceivedAtUtc = message.ReceivedAtUtc.UtcDateTime,
            ProcessingStartedAtUtc = message.ProcessingStartedAtUtc?.UtcDateTime,
            ProcessedAtUtc = message.ProcessedAtUtc?.UtcDateTime,
            DeadLetteredAtUtc = message.DeadLetteredAtUtc?.UtcDateTime,
            NextRetryAtUtc = message.NextRetryAtUtc?.UtcDateTime,
            LeaseOwner = message.LeaseOwner,
            LeaseExpiresAtUtc = message.LeaseExpiresAtUtc?.UtcDateTime,
            FailureReason = message.FailureReason,
            TerminalAtUtc = message.Status switch
            {
                InboxMessageStatus.Processed => message.ProcessedAtUtc?.UtcDateTime,
                InboxMessageStatus.DeadLettered => message.DeadLetteredAtUtc?.UtcDateTime,
                _ => null,
            },
            Version = version,
        };

        public static InboxMessage ToDomain(InboxMessageDocument document) => new()
        {
            Id = Guid.ParseExact(document.Id, "N"),
            MessageId = document.MessageId,
            ChannelKey = Guid.ParseExact(document.ChannelKey, "N"),
            FeederId = Guid.ParseExact(document.FeederId, "N"),
            PartitionKey = document.PartitionKey,
            SchemaVersion = document.SchemaVersion,
            PayloadContentType = document.PayloadContentType,
            Payload = document.Payload,
            Headers = document.Headers,
            Status = document.Status,
            AttemptCount = document.AttemptCount,
            ReceivedAtUtc = AsUtcOffset(document.ReceivedAtUtc),
            ProcessingStartedAtUtc = AsUtcOffset(document.ProcessingStartedAtUtc),
            ProcessedAtUtc = AsUtcOffset(document.ProcessedAtUtc),
            DeadLetteredAtUtc = AsUtcOffset(document.DeadLetteredAtUtc),
            NextRetryAtUtc = AsUtcOffset(document.NextRetryAtUtc),
            LeaseOwner = document.LeaseOwner,
            LeaseExpiresAtUtc = AsUtcOffset(document.LeaseExpiresAtUtc),
            FailureReason = document.FailureReason,
        };

        private static DateTimeOffset AsUtcOffset(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));

        private static DateTimeOffset? AsUtcOffset(DateTime? value) => value is { } v ? AsUtcOffset(v) : null;
    }
}
