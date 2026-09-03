namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Everything an <see cref="IInboxStore"/> needs to atomically record-or-claim one inbound
    /// message. See <see cref="IInboxStore.TryClaimAsync"/>.
    /// </summary>
    public sealed record InboxClaimRequest
    {
        /// <summary>The caller/business message identifier the dedup key is built from.</summary>
        public required string MessageId { get; init; }

        /// <summary>The channel this message was received on.</summary>
        public required Guid ChannelKey { get; init; }

        /// <summary>The specific feeder instance receiving this message.</summary>
        public required Guid FeederId { get; init; }

        /// <summary>Optional additional dedup/ordering scope beneath channel+feeder.</summary>
        public string? PartitionKey { get; init; }

        /// <summary>Schema version of <see cref="Payload"/>.</summary>
        public required int SchemaVersion { get; init; }

        /// <summary>Content type of <see cref="Payload"/>.</summary>
        public required string PayloadContentType { get; init; }

        /// <summary>The raw, serialized message payload.</summary>
        public required byte[] Payload { get; init; }

        /// <summary>Optional bounded header set.</summary>
        public IReadOnlyDictionary<string, string>? Headers { get; init; }

        /// <summary>Opaque token identifying the caller, stamped as the lease owner on success.</summary>
        public required string LeaseOwner { get; init; }

        /// <summary>How long the processing lease should be held for.</summary>
        public required TimeSpan LeaseDuration { get; init; }
    }
}
