using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// A durable, versioned record of one outbound message moving through the Outbox state
    /// machine (see <see cref="OutboxMessageStatus"/>). Instances are immutable snapshots -
    /// <see cref="TryTransitionTo"/> and <see cref="Requeue"/> return a new snapshot rather than
    /// mutating the current one.
    /// </summary>
    /// <remarks>
    /// The copy constructor backing the compiler-generated <c>with</c> expression is deliberately
    /// <c>private</c>: every field change must go through <see cref="TryTransitionTo"/> or
    /// <see cref="Requeue"/> so the transition table below is the single place transitions are
    /// validated. Constructing the very first snapshot for a newly-enqueued message goes through
    /// the explicit <see cref="CreatePending"/> factory instead of a public constructor.
    /// </remarks>
    public sealed record OutboxMessage
    {
        // from -> allowed targets. Published/DeadLettered are terminal (absent as a key below);
        // the only way back from them is the explicit Requeue escape hatch, not a normal transition.
        private static readonly Dictionary<OutboxMessageStatus, OutboxMessageStatus[]> AllowedTransitions = new()
        {
            [OutboxMessageStatus.Pending] = [OutboxMessageStatus.Publishing],
            [OutboxMessageStatus.Publishing] = [OutboxMessageStatus.Published, OutboxMessageStatus.Failed, OutboxMessageStatus.DeadLettered],
            [OutboxMessageStatus.Failed] = [OutboxMessageStatus.Publishing, OutboxMessageStatus.DeadLettered],
        };

        /// <summary>Store-assigned identity of this record. Stable across transitions.</summary>
        public required Guid Id { get; init; }

        /// <summary>Stable, caller-supplied-or-generated idempotency/message identifier.</summary>
        public required string MessageId { get; init; }

        /// <summary>The publishing provider/destination this message targets.</summary>
        public required string ProviderKey { get; init; }

        /// <summary>
        /// The ordering partition this message belongs to. Ordering is only guaranteed within a
        /// partition, not globally - see <see cref="OrderingSequence"/>.
        /// </summary>
        public string? PartitionKey { get; init; }

        /// <summary>
        /// Monotonically increasing sequence assigned by the store, scoped to
        /// <see cref="PartitionKey"/>, that a relay worker orders its claims by.
        /// </summary>
        public long OrderingSequence { get; init; }

        /// <summary>Schema version of <see cref="Payload"/>, for forward/backward compatibility.</summary>
        public required int SchemaVersion { get; init; }

        /// <summary>Content type of <see cref="Payload"/> (e.g. "application/json").</summary>
        public required string PayloadContentType { get; init; }

        /// <summary>The raw, serialized message payload. Bounded by <see cref="OutboxMessageLimits.MaxPayloadSizeBytes"/>.</summary>
        public required byte[] Payload { get; init; }

        /// <summary>
        /// Bounded header set. Never null - empty when there are no headers. Always enumerates in
        /// ascending ordinal key order (see <see cref="NormalizeHeaders"/>) regardless of the order
        /// headers were supplied in, so two messages with the same logical headers serialize identically.
        /// </summary>
        public IReadOnlyDictionary<string, string> Headers { get; init; } = NormalizeHeaders(null);

        /// <summary>Current lifecycle state.</summary>
        public required OutboxMessageStatus Status { get; init; }

        /// <summary>Number of publish attempts made so far (incremented on each claim).</summary>
        public int Attempts { get; init; }

        /// <summary>When the message was enqueued.</summary>
        public required DateTimeOffset CreatedAtUtc { get; init; }

        /// <summary>When the current/most recent publish attempt started.</summary>
        public DateTimeOffset? PublishingStartedAtUtc { get; init; }

        /// <summary>When the message reached <see cref="OutboxMessageStatus.Published"/>.</summary>
        public DateTimeOffset? PublishedAtUtc { get; init; }

        /// <summary>
        /// When the message reached <see cref="OutboxMessageStatus.DeadLettered"/>. A purge/retention
        /// worker ages a dead-lettered entry off this timestamp, the same way it ages a
        /// <see cref="OutboxMessageStatus.Published"/> entry off <see cref="PublishedAtUtc"/>.
        /// </summary>
        public DateTimeOffset? DeadLetteredAtUtc { get; init; }

        /// <summary>Earliest time a retry claim may succeed, when <see cref="Status"/> is <see cref="OutboxMessageStatus.Failed"/>.</summary>
        public DateTimeOffset? NextRetryAtUtc { get; init; }

        /// <summary>Opaque token identifying whoever holds the current publishing lease, if any.</summary>
        public string? LeaseOwner { get; init; }

        /// <summary>When the current lease expires and becomes reclaimable by another relay worker.</summary>
        public DateTimeOffset? LeaseExpiresAtUtc { get; init; }

        /// <summary>
        /// Sanitized (no stack traces/secrets) failure summary. Bounded by
        /// <see cref="OutboxMessageLimits.MaxFailureReasonLength"/>.
        /// </summary>
        public string? FailureReason { get; init; }

        /// <summary>Private copy constructor - disables the public <c>with</c> expression outside this file.</summary>
        [SetsRequiredMembers]
        private OutboxMessage(OutboxMessage original)
        {
            Id = original.Id;
            MessageId = original.MessageId;
            ProviderKey = original.ProviderKey;
            PartitionKey = original.PartitionKey;
            OrderingSequence = original.OrderingSequence;
            SchemaVersion = original.SchemaVersion;
            PayloadContentType = original.PayloadContentType;
            Payload = original.Payload;
            Headers = original.Headers;
            Status = original.Status;
            Attempts = original.Attempts;
            CreatedAtUtc = original.CreatedAtUtc;
            PublishingStartedAtUtc = original.PublishingStartedAtUtc;
            PublishedAtUtc = original.PublishedAtUtc;
            DeadLetteredAtUtc = original.DeadLetteredAtUtc;
            NextRetryAtUtc = original.NextRetryAtUtc;
            LeaseOwner = original.LeaseOwner;
            LeaseExpiresAtUtc = original.LeaseExpiresAtUtc;
            FailureReason = original.FailureReason;
        }

        public OutboxMessage()
        {
        }

        /// <summary>Builds the first snapshot for a newly-enqueued message, in <see cref="OutboxMessageStatus.Pending"/>.</summary>
        public static OutboxMessage CreatePending(
            Guid id,
            string messageId,
            string providerKey,
            long orderingSequence,
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
                ProviderKey = providerKey,
                PartitionKey = partitionKey,
                OrderingSequence = orderingSequence,
                SchemaVersion = schemaVersion,
                PayloadContentType = payloadContentType,
                Payload = payload,
                Headers = NormalizeHeaders(headers),
                Status = OutboxMessageStatus.Pending,
                Attempts = 0,
                CreatedAtUtc = timeProvider.GetUtcNow(),
            };

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

        /// <summary>Whether a normal (non-requeue) transition from <see cref="Status"/> to <paramref name="target"/> is allowed.</summary>
        public bool CanTransitionTo(OutboxMessageStatus target) =>
            AllowedTransitions.TryGetValue(Status, out var targets) && Array.IndexOf(targets, target) >= 0;

        /// <summary>
        /// Attempts a normal transition, returning the new snapshot on success. Throws
        /// <see cref="OutboxMessageTransitionException"/> when <see cref="CanTransitionTo"/> would
        /// return <see langword="false"/>.
        /// </summary>
        public OutboxMessage TryTransitionTo(
            OutboxMessageStatus target,
            TimeProvider timeProvider,
            string? leaseOwner = null,
            DateTimeOffset? leaseExpiresAtUtc = null,
            DateTimeOffset? nextRetryAtUtc = null,
            string? failureReason = null,
            bool incrementAttempt = false)
        {
            if (!CanTransitionTo(target))
                throw new OutboxMessageTransitionException(Status, target);

            var now = timeProvider.GetUtcNow();

            return target switch
            {
                OutboxMessageStatus.Publishing => this with
                {
                    Status = target,
                    Attempts = incrementAttempt ? Attempts + 1 : Attempts,
                    PublishingStartedAtUtc = now,
                    LeaseOwner = leaseOwner,
                    LeaseExpiresAtUtc = leaseExpiresAtUtc,
                    NextRetryAtUtc = null,
                    FailureReason = null,
                },
                OutboxMessageStatus.Published => this with
                {
                    Status = target,
                    PublishedAtUtc = now,
                    LeaseOwner = null,
                    LeaseExpiresAtUtc = null,
                    NextRetryAtUtc = null,
                    FailureReason = null,
                },
                OutboxMessageStatus.Failed => this with
                {
                    Status = target,
                    LeaseOwner = null,
                    LeaseExpiresAtUtc = null,
                    NextRetryAtUtc = nextRetryAtUtc,
                    FailureReason = Truncate(failureReason),
                },
                OutboxMessageStatus.DeadLettered => this with
                {
                    Status = target,
                    DeadLetteredAtUtc = now,
                    LeaseOwner = null,
                    LeaseExpiresAtUtc = null,
                    NextRetryAtUtc = null,
                    FailureReason = Truncate(failureReason),
                },
                _ => throw new OutboxMessageTransitionException(Status, target),
            };
        }

        /// <summary>
        /// The one sanctioned way back from a terminal state (<see cref="OutboxMessageStatus.Published"/>
        /// or <see cref="OutboxMessageStatus.DeadLettered"/>) to <see cref="OutboxMessageStatus.Pending"/>,
        /// for deliberate operator/tooling-driven republishing. Never called implicitly by store or
        /// relay worker code. Assigns a new <see cref="OrderingSequence"/> - a requeued message is
        /// ordered after everything currently pending in its partition, not replayed in place.
        /// </summary>
        public OutboxMessage Requeue(long newOrderingSequence, TimeProvider timeProvider)
        {
            if (Status is not (OutboxMessageStatus.Published or OutboxMessageStatus.DeadLettered))
                throw new OutboxMessageTransitionException(Status, OutboxMessageStatus.Pending);

            return this with
            {
                Status = OutboxMessageStatus.Pending,
                OrderingSequence = newOrderingSequence,
                Attempts = 0,
                PublishingStartedAtUtc = null,
                PublishedAtUtc = null,
                DeadLetteredAtUtc = null,
                NextRetryAtUtc = null,
                LeaseOwner = null,
                LeaseExpiresAtUtc = null,
                FailureReason = null,
                CreatedAtUtc = timeProvider.GetUtcNow(),
            };
        }

        /// <summary>
        /// Extends an active publishing lease's expiry without otherwise changing this snapshot - the
        /// one sanctioned mutation outside <see cref="TryTransitionTo"/>'s transition table, since a
        /// heartbeat renewal is not itself a lifecycle transition. Backs <see cref="IOutboxStore.RenewLeaseAsync"/>.
        /// </summary>
        /// <returns>
        /// The renewed snapshot, or <see langword="null"/> if <see cref="Status"/> is not
        /// <see cref="OutboxMessageStatus.Publishing"/> or <paramref name="leaseOwner"/> does not match
        /// <see cref="LeaseOwner"/> - the same compare-and-swap check every other lease-scoped operation
        /// performs, so a relay worker that lost its lease can never renew it either.
        /// </returns>
        public OutboxMessage? RenewLease(TimeProvider timeProvider, string leaseOwner, TimeSpan leaseExtension)
        {
            if (Status != OutboxMessageStatus.Publishing || LeaseOwner != leaseOwner)
                return null;

            return this with { LeaseExpiresAtUtc = timeProvider.GetUtcNow() + leaseExtension };
        }

        private static string? Truncate(string? failureReason) =>
            failureReason is { Length: > OutboxMessageLimits.MaxFailureReasonLength }
                ? failureReason[..OutboxMessageLimits.MaxFailureReasonLength]
                : failureReason;
    }
}
