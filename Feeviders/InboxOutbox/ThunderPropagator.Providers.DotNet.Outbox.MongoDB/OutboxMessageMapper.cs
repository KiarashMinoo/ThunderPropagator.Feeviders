namespace ThunderPropagator.Providers.DotNet.Outbox.MongoDB
{
    /// <summary>Converts between <see cref="OutboxMessage"/> and its persisted <see cref="OutboxMessageDocument"/> shape.</summary>
    internal static class OutboxMessageMapper
    {
        public static OutboxMessageDocument ToDocument(OutboxMessage message, long version) => new()
        {
            Id = message.Id.ToString("N"),
            MessageId = message.MessageId,
            ProviderKey = message.ProviderKey,
            PartitionKey = message.PartitionKey,
            OrderingSequence = message.OrderingSequence,
            SchemaVersion = message.SchemaVersion,
            PayloadContentType = message.PayloadContentType,
            Payload = message.Payload,
            Headers = new Dictionary<string, string>(message.Headers),
            Status = message.Status,
            Attempts = message.Attempts,
            CreatedAtUtc = message.CreatedAtUtc.UtcDateTime,
            PublishingStartedAtUtc = message.PublishingStartedAtUtc?.UtcDateTime,
            PublishedAtUtc = message.PublishedAtUtc?.UtcDateTime,
            DeadLetteredAtUtc = message.DeadLetteredAtUtc?.UtcDateTime,
            NextRetryAtUtc = message.NextRetryAtUtc?.UtcDateTime,
            LeaseOwner = message.LeaseOwner,
            LeaseExpiresAtUtc = message.LeaseExpiresAtUtc?.UtcDateTime,
            FailureReason = message.FailureReason,
            TerminalAtUtc = message.Status switch
            {
                OutboxMessageStatus.Published => message.PublishedAtUtc?.UtcDateTime,
                OutboxMessageStatus.DeadLettered => message.DeadLetteredAtUtc?.UtcDateTime,
                _ => null,
            },
            Version = version,
        };

        public static OutboxMessage ToDomain(OutboxMessageDocument document) => new()
        {
            Id = Guid.ParseExact(document.Id, "N"),
            MessageId = document.MessageId,
            ProviderKey = document.ProviderKey,
            PartitionKey = document.PartitionKey,
            OrderingSequence = document.OrderingSequence,
            SchemaVersion = document.SchemaVersion,
            PayloadContentType = document.PayloadContentType,
            Payload = document.Payload,
            Headers = document.Headers,
            Status = document.Status,
            Attempts = document.Attempts,
            CreatedAtUtc = AsUtcOffset(document.CreatedAtUtc),
            PublishingStartedAtUtc = AsUtcOffset(document.PublishingStartedAtUtc),
            PublishedAtUtc = AsUtcOffset(document.PublishedAtUtc),
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
