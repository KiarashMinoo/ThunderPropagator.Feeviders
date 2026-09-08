namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Everything an <see cref="IInboxDeadLetterHandler"/> needs about one entry that just reached
    /// <see cref="InboxMessageStatus.DeadLettered"/> - built once (see <see cref="InboxDeadLetterContextFactory"/>)
    /// and handed to every handler in the pipeline, so handlers never need their own <see cref="IInboxStore"/>
    /// reference just to look up what happened.
    /// </summary>
    public sealed record InboxDeadLetterContext
    {
        /// <summary>The store-assigned identity of the dead-lettered entry - <see cref="InboxMessage.Id"/>.</summary>
        public required Guid Id { get; init; }

        /// <summary>The caller/business message identifier.</summary>
        public required string MessageId { get; init; }

        /// <summary>The channel the message was received on.</summary>
        public required Guid ChannelKey { get; init; }

        /// <summary>The feeder instance that received it - see <see cref="Source"/>.</summary>
        public required Guid FeederId { get; init; }

        /// <summary>The dedup/ordering partition beneath channel+feeder, if any.</summary>
        public string? PartitionKey { get; init; }

        /// <summary>Where this message came from - the feeder instance that received it.</summary>
        public string Source => FeederId.ToString();

        /// <summary>Where this message was headed - the channel its consumers read from.</summary>
        public string Target => ChannelKey.ToString();

        /// <summary>Number of processing attempts made before this entry was dead-lettered.</summary>
        public required int AttemptCount { get; init; }

        /// <summary>When the message was first durably recorded.</summary>
        public required DateTimeOffset ReceivedAtUtc { get; init; }

        /// <summary>When the message reached <see cref="InboxMessageStatus.DeadLettered"/>.</summary>
        public required DateTimeOffset DeadLetteredAtUtc { get; init; }

        /// <summary>Why this entry was dead-lettered - see <see cref="InboxFailureClassifier"/>.</summary>
        public required InboxDeadLetterFailureCategory FailureCategory { get; init; }

        /// <summary>The same sanitized (type-name-only, never the raw exception message) reason persisted on the entry.</summary>
        public required string FailureReason { get; init; }

        /// <summary>
        /// The <see cref="System.Diagnostics.Activity.Id"/> in effect when this entry was dead-lettered,
        /// if any - correlates a dead-letter record back to the distributed trace that produced it.
        /// </summary>
        public string? TraceId { get; init; }

        /// <summary>
        /// The original payload, or <see langword="null"/> when <see cref="InboxDeadLetterPayloadPolicy"/>
        /// excluded it - see <see cref="InboxDeadLetterContextFactory"/>'s remarks.
        /// </summary>
        public byte[]? Payload { get; init; }

        /// <summary>Content type describing <see cref="Payload"/>, when present.</summary>
        public string? PayloadContentType { get; init; }

        /// <summary>The original bounded header set.</summary>
        public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();
    }
}
