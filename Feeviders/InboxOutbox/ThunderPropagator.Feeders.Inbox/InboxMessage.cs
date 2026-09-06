using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// A durable, versioned record of one inbound message moving through the Inbox state
    /// machine (see <see cref="InboxMessageStatus"/>). Instances are immutable snapshots -
    /// <see cref="TryTransitionTo"/> and <see cref="Replay"/> return a new snapshot rather than
    /// mutating the current one, so a caller can never observe a half-applied transition and a
    /// store can persist/return a snapshot without also handing out a live, mutable reference.
    /// </summary>
    /// <remarks>
    /// The explicit copy constructor is <c>private</c> so <c>new InboxMessage(otherMessage)</c> cannot
    /// be called from outside this file - but this does not lock down <c>with</c> itself: a record's
    /// compiler-synthesized clone method that backs <c>with</c> remains public regardless of the copy
    /// constructor's declared accessibility, so <c>with</c> still compiles for any caller. There is no
    /// compiler-enforced guarantee here, only a convention: callers outside this file should treat
    /// every field change as going through <see cref="TryTransitionTo"/>, <see cref="Replay"/>, or
    /// <see cref="RenewLease"/> - the only members that validate the transition table below (or, for
    /// <see cref="RenewLease"/>, the one documented exception to it) - rather than an ad hoc <c>with</c>.
    /// Constructing the very first snapshot for a newly-received message goes through the explicit
    /// <see cref="CreateReceived"/> factory instead of a public constructor.
    /// </remarks>
    public sealed record InboxMessage
    {
        // from -> allowed targets. Processed/DeadLettered are terminal (absent as a key below);
        // the only way back from them is the explicit Replay escape hatch, not a normal transition.
        private static readonly Dictionary<InboxMessageStatus, InboxMessageStatus[]> AllowedTransitions = new()
        {
            [InboxMessageStatus.Received] = [InboxMessageStatus.Processing],
            [InboxMessageStatus.Processing] = [InboxMessageStatus.Processed, InboxMessageStatus.Failed, InboxMessageStatus.DeadLettered],
            [InboxMessageStatus.Failed] = [InboxMessageStatus.Processing, InboxMessageStatus.DeadLettered],
        };

        /// <summary>Store-assigned identity of this record. Stable across transitions.</summary>
        public required Guid Id { get; init; }

        /// <summary>
        /// The caller/business message identifier the dedup key is built from. Combined with
        /// <see cref="ChannelKey"/> and <see cref="PartitionKey"/> to scope uniqueness - see
        /// <see cref="IInboxStore.TryClaimAsync"/>.
        /// </summary>
        public required string MessageId { get; init; }

        /// <summary>The channel this message was received on.</summary>
        public required Guid ChannelKey { get; init; }

        /// <summary>The specific feeder instance that received this message.</summary>
        public required Guid FeederId { get; init; }

        /// <summary>Optional additional dedup/ordering scope beneath channel+feeder.</summary>
        public string? PartitionKey { get; init; }

        /// <summary>
        /// Schema version of <see cref="Payload"/>. Compatibility contract: a given <see cref="SchemaVersion"/>
        /// value must always describe the same wire shape for <see cref="PayloadContentType"/> - once
        /// published, a version number is immutable and is never reused for an incompatible shape. Producers
        /// increment it on any breaking change; consumers that do not recognize a <see cref="SchemaVersion"/>
        /// must treat the message as undecodable (typically routing it to <see cref="InboxMessageStatus.DeadLettered"/>
        /// via <see cref="IInboxStore.DeadLetterAsync"/>) rather than guessing at the shape.
        /// </summary>
        public required int SchemaVersion { get; init; }

        /// <summary>Content type of <see cref="Payload"/> (e.g. "application/json").</summary>
        public required string PayloadContentType { get; init; }

        /// <summary>The raw, serialized message payload. Bounded by <see cref="InboxMessageLimits.MaxPayloadSizeBytes"/>.</summary>
        public required byte[] Payload { get; init; }

        /// <summary>
        /// Bounded header set. Never null - empty when there are no headers. Always enumerates in
        /// ascending ordinal key order (see <see cref="NormalizeHeaders"/>) regardless of the order headers
        /// were supplied in, so two messages with the same logical headers serialize identically.
        /// </summary>
        public IReadOnlyDictionary<string, string> Headers { get; init; } = NormalizeHeaders(null);

        /// <summary>Current lifecycle state.</summary>
        public required InboxMessageStatus Status { get; init; }

        /// <summary>Number of processing attempts made so far (incremented on each claim).</summary>
        public int AttemptCount { get; init; }

        /// <summary>When the message was first durably recorded.</summary>
        public required DateTimeOffset ReceivedAtUtc { get; init; }

        /// <summary>When the current/most recent processing attempt started.</summary>
        public DateTimeOffset? ProcessingStartedAtUtc { get; init; }

        /// <summary>When the message reached <see cref="InboxMessageStatus.Processed"/>.</summary>
        public DateTimeOffset? ProcessedAtUtc { get; init; }

        /// <summary>
        /// When the message reached <see cref="InboxMessageStatus.DeadLettered"/>. Purge operations
        /// (<see cref="IInboxStore.PurgeAsync"/>) age a dead-lettered entry off this timestamp, the
        /// same way they age a <see cref="InboxMessageStatus.Processed"/> entry off <see cref="ProcessedAtUtc"/>.
        /// </summary>
        public DateTimeOffset? DeadLetteredAtUtc { get; init; }

        /// <summary>Earliest time a retry claim may succeed, when <see cref="Status"/> is <see cref="InboxMessageStatus.Failed"/>.</summary>
        public DateTimeOffset? NextRetryAtUtc { get; init; }

        /// <summary>Opaque token identifying whoever holds the current processing lease, if any.</summary>
        public string? LeaseOwner { get; init; }

        /// <summary>When the current lease expires and becomes reclaimable by another worker.</summary>
        public DateTimeOffset? LeaseExpiresAtUtc { get; init; }

        /// <summary>
        /// Sanitized (no stack traces/secrets) failure summary. Bounded by
        /// <see cref="InboxMessageLimits.MaxFailureReasonLength"/>.
        /// </summary>
        public string? FailureReason { get; init; }

        /// <summary>
        /// Private copy constructor - blocks <c>new InboxMessage(otherMessage)</c> from outside this
        /// file. Does not block <c>with</c>; see the class remarks.
        /// </summary>
        [SetsRequiredMembers]
        private InboxMessage(InboxMessage original)
        {
            Id = original.Id;
            MessageId = original.MessageId;
            ChannelKey = original.ChannelKey;
            FeederId = original.FeederId;
            PartitionKey = original.PartitionKey;
            SchemaVersion = original.SchemaVersion;
            PayloadContentType = original.PayloadContentType;
            Payload = original.Payload;
            Headers = original.Headers;
            Status = original.Status;
            AttemptCount = original.AttemptCount;
            ReceivedAtUtc = original.ReceivedAtUtc;
            ProcessingStartedAtUtc = original.ProcessingStartedAtUtc;
            ProcessedAtUtc = original.ProcessedAtUtc;
            DeadLetteredAtUtc = original.DeadLetteredAtUtc;
            NextRetryAtUtc = original.NextRetryAtUtc;
            LeaseOwner = original.LeaseOwner;
            LeaseExpiresAtUtc = original.LeaseExpiresAtUtc;
            FailureReason = original.FailureReason;
        }

        public InboxMessage()
        {
        }

        /// <summary>Builds the first snapshot for a newly-received message, in <see cref="InboxMessageStatus.Received"/>.</summary>
        public static InboxMessage CreateReceived(
            Guid id,
            string messageId,
            Guid channelKey,
            Guid feederId,
            int schemaVersion,
            string payloadContentType,
            byte[] payload,
            IReadOnlyDictionary<string, string>? headers,
            string? partitionKey,
            TimeProvider timeProvider) =>
            new()
            {
                Id = id,
                MessageId = messageId,
                ChannelKey = channelKey,
                FeederId = feederId,
                PartitionKey = partitionKey,
                SchemaVersion = schemaVersion,
                PayloadContentType = payloadContentType,
                Payload = payload,
                Headers = NormalizeHeaders(headers),
                Status = InboxMessageStatus.Received,
                AttemptCount = 0,
                ReceivedAtUtc = timeProvider.GetUtcNow(),
            };

        /// <summary>Whether a normal (non-replay) transition from <see cref="Status"/> to <paramref name="target"/> is allowed.</summary>
        public bool CanTransitionTo(InboxMessageStatus target) =>
            AllowedTransitions.TryGetValue(Status, out var targets) && Array.IndexOf(targets, target) >= 0;

        /// <summary>
        /// Attempts a normal transition, returning the new snapshot on success. Throws
        /// <see cref="InboxMessageTransitionException"/> when <see cref="CanTransitionTo"/> would
        /// return <see langword="false"/> - callers that treat an invalid transition as an
        /// expected possibility rather than a bug should check <see cref="CanTransitionTo"/> first.
        /// </summary>
        public InboxMessage TryTransitionTo(
            InboxMessageStatus target,
            TimeProvider timeProvider,
            string? leaseOwner = null,
            DateTimeOffset? leaseExpiresAtUtc = null,
            DateTimeOffset? nextRetryAtUtc = null,
            string? failureReason = null,
            bool incrementAttempt = false)
        {
            if (!CanTransitionTo(target))
                throw new InboxMessageTransitionException(Status, target);

            var now = timeProvider.GetUtcNow();

            return target switch
            {
                InboxMessageStatus.Processing => this with
                {
                    Status = target,
                    AttemptCount = incrementAttempt ? AttemptCount + 1 : AttemptCount,
                    ProcessingStartedAtUtc = now,
                    LeaseOwner = leaseOwner,
                    LeaseExpiresAtUtc = leaseExpiresAtUtc,
                    NextRetryAtUtc = null,
                    FailureReason = null,
                },
                InboxMessageStatus.Processed => this with
                {
                    Status = target,
                    ProcessedAtUtc = now,
                    LeaseOwner = null,
                    LeaseExpiresAtUtc = null,
                    NextRetryAtUtc = null,
                    FailureReason = null,
                },
                InboxMessageStatus.Failed => this with
                {
                    Status = target,
                    LeaseOwner = null,
                    LeaseExpiresAtUtc = null,
                    NextRetryAtUtc = nextRetryAtUtc,
                    FailureReason = Truncate(failureReason),
                },
                InboxMessageStatus.DeadLettered => this with
                {
                    Status = target,
                    DeadLetteredAtUtc = now,
                    LeaseOwner = null,
                    LeaseExpiresAtUtc = null,
                    NextRetryAtUtc = null,
                    FailureReason = Truncate(failureReason),
                },
                _ => throw new InboxMessageTransitionException(Status, target),
            };
        }

        /// <summary>
        /// The one sanctioned way back from a terminal state (<see cref="InboxMessageStatus.Processed"/>
        /// or <see cref="InboxMessageStatus.DeadLettered"/>) to <see cref="InboxMessageStatus.Received"/>,
        /// for deliberate operator/tooling-driven reprocessing. Never called implicitly by store or
        /// worker code.
        /// </summary>
        public InboxMessage Replay(TimeProvider timeProvider)
        {
            if (Status is not (InboxMessageStatus.Processed or InboxMessageStatus.DeadLettered))
                throw new InboxMessageTransitionException(Status, InboxMessageStatus.Received);

            return this with
            {
                Status = InboxMessageStatus.Received,
                AttemptCount = 0,
                ProcessingStartedAtUtc = null,
                ProcessedAtUtc = null,
                DeadLetteredAtUtc = null,
                NextRetryAtUtc = null,
                LeaseOwner = null,
                LeaseExpiresAtUtc = null,
                FailureReason = null,
                ReceivedAtUtc = timeProvider.GetUtcNow(),
            };
        }

        /// <summary>
        /// Extends an active processing lease's expiry without otherwise changing this snapshot - the
        /// one sanctioned mutation outside <see cref="TryTransitionTo"/>'s transition table, since a
        /// heartbeat renewal is not itself a lifecycle transition. Backs <see cref="IInboxStore.RenewLeaseAsync"/>.
        /// </summary>
        /// <returns>
        /// The renewed snapshot, or <see langword="null"/> if <see cref="Status"/> is not
        /// <see cref="InboxMessageStatus.Processing"/> or <paramref name="leaseOwner"/> does not match
        /// <see cref="LeaseOwner"/> - the same compare-and-swap check every other lease-scoped operation
        /// performs, so a worker that lost its lease can never renew it either.
        /// </returns>
        public InboxMessage? RenewLease(TimeProvider timeProvider, string leaseOwner, TimeSpan leaseExtension)
        {
            if (Status != InboxMessageStatus.Processing || LeaseOwner != leaseOwner)
                return null;

            return this with { LeaseExpiresAtUtc = timeProvider.GetUtcNow() + leaseExtension };
        }

        private static string? Truncate(string? failureReason) =>
            failureReason is { Length: > InboxMessageLimits.MaxFailureReasonLength }
                ? failureReason[..InboxMessageLimits.MaxFailureReasonLength]
                : failureReason;

        private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
            new ReadOnlyDictionary<string, string>(new Dictionary<string, string>());

        /// <summary>
        /// Copies <paramref name="headers"/> into a key-sorted (ordinal), read-only dictionary so
        /// <see cref="Headers"/> always enumerates - and therefore serializes - in the same order
        /// regardless of the order the caller supplied entries in.
        /// </summary>
        private static IReadOnlyDictionary<string, string> NormalizeHeaders(IReadOnlyDictionary<string, string>? headers) =>
            headers is null or { Count: 0 }
                ? EmptyHeaders
                : new ReadOnlyDictionary<string, string>(new SortedDictionary<string, string>(new Dictionary<string, string>(headers), StringComparer.Ordinal));
    }
}
