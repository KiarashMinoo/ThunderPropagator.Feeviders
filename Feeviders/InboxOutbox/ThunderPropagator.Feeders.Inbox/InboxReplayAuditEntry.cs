namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>One durable record of a manual replay attempt - written by <see cref="InboxDeadLetterReplayService"/> via <see cref="IInboxReplayAuditSink"/>, whether or not the attempt succeeded.</summary>
    public sealed record InboxReplayAuditEntry
    {
        /// <summary>The store-assigned identity of the entry a replay was requested for.</summary>
        public required Guid Id { get; init; }

        /// <summary>The caller/business message identifier - see <see cref="InboxMessage.MessageId"/>.</summary>
        public required string MessageId { get; init; }

        /// <summary>The channel the entry belongs to.</summary>
        public required Guid ChannelKey { get; init; }

        /// <summary>Opaque identity of whoever requested the replay - never interpreted by this library, only recorded.</summary>
        public required string RequestedBy { get; init; }

        /// <summary>When the replay was requested.</summary>
        public required DateTimeOffset RequestedAtUtc { get; init; }

        /// <summary>Whether <see cref="IInboxReplayAuthorizer"/> allowed this request.</summary>
        public required bool Authorized { get; init; }

        /// <summary>Why authorization was denied, when <see cref="Authorized"/> is <see langword="false"/>.</summary>
        public string? DenialReason { get; init; }

        /// <summary>Whether the entry was actually transitioned back to <see cref="InboxMessageStatus.Received"/>. Always <see langword="false"/> when <see cref="Authorized"/> is <see langword="false"/>.</summary>
        public required bool Succeeded { get; init; }

        /// <summary>Why the replay itself failed (e.g. the entry was no longer DeadLettered by the time it ran), when authorized but not <see cref="Succeeded"/>.</summary>
        public string? FailureReason { get; init; }
    }
}
