namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Atomic, backend-independent Inbox persistence contract. Every method returns an immutable
    /// <see cref="InboxMessage"/> snapshot (or <see langword="null"/> where the operation could not
    /// be applied) rather than mutating a shared instance, so callers never observe a torn state.
    /// </summary>
    /// <remarks>
    /// Replaces the check-then-save (<c>ExistsAsync</c> + <c>SaveAsync</c>) pattern with a single
    /// atomic <see cref="TryClaimAsync"/> call: implementations must guarantee that two concurrent
    /// calls with the same dedup key (<see cref="InboxClaimRequest.MessageId"/> scoped by
    /// <see cref="InboxClaimRequest.ChannelKey"/>/<see cref="InboxClaimRequest.PartitionKey"/>)
    /// yield exactly one <see cref="InboxClaimOutcome.Claimed"/> result. Every method that mutates
    /// an existing entry (<see cref="CompleteAsync"/>, <see cref="FailAsync"/>,
    /// <see cref="DeadLetterAsync"/>, <see cref="RenewLeaseAsync"/>) must verify the supplied
    /// <c>leaseOwner</c> still matches the current lease as part of the same atomic operation
    /// (compare-and-swap), returning <see langword="null"/> instead of applying the change when it
    /// does not - a lease that lapsed and was reclaimed by another worker must never be
    /// completed/failed by the worker that lost it.
    /// </remarks>
    public interface IInboxStore
    {
        /// <summary>
        /// Atomically records a new message or claims an existing, retry-eligible one for
        /// processing. See the outcomes on <see cref="InboxClaimOutcome"/> for what each result means.
        /// </summary>
        Task<InboxClaimResult> TryClaimAsync(InboxClaimRequest request, CancellationToken cancellationToken = default);

        /// <summary>Looks up the current snapshot for a dedup key, without claiming it.</summary>
        Task<InboxMessage?> GetAsync(string messageId, Guid channelKey, string? partitionKey, CancellationToken cancellationToken = default);

        /// <summary>
        /// Marks a leased entry <see cref="InboxMessageStatus.Processed"/>. Returns
        /// <see langword="null"/> if <paramref name="leaseOwner"/> no longer matches the current lease.
        /// </summary>
        Task<InboxMessage?> CompleteAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default);

        /// <summary>
        /// Marks a leased entry <see cref="InboxMessageStatus.Failed"/> and releases the lease, so a
        /// retry worker can claim it again at/after <paramref name="nextRetryAtUtc"/> - or immediately,
        /// if <paramref name="nextRetryAtUtc"/> is <see langword="null"/> (no backoff delay). Every
        /// implementation must treat a <see langword="null"/> <see cref="InboxMessage.NextRetryAtUtc"/>
        /// as already elapsed for both <see cref="TryClaimAsync"/>'s reclaim check and
        /// <see cref="QueryRetryableAsync"/>, so retry eligibility is consistent across backends.
        /// Returns <see langword="null"/> if <paramref name="leaseOwner"/> no longer matches the
        /// current lease.
        /// </summary>
        Task<InboxMessage?> FailAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default);

        /// <summary>
        /// Marks a leased entry <see cref="InboxMessageStatus.DeadLettered"/> (retry policy exhausted,
        /// or the failure was flagged non-retryable). Returns <see langword="null"/> if
        /// <paramref name="leaseOwner"/> no longer matches the current lease.
        /// </summary>
        Task<InboxMessage?> DeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default);

        /// <summary>
        /// Extends an active lease's expiry, for long-running handlers - typically implemented via
        /// <see cref="InboxMessage.RenewLease"/>, which performs the same compare-and-swap check.
        /// Returns <see langword="null"/> if <paramref name="leaseOwner"/> no longer matches the
        /// current lease.
        /// </summary>
        Task<InboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns entries a retry worker should attempt to reclaim via <see cref="TryClaimAsync"/>:
        /// entries in <see cref="InboxMessageStatus.Failed"/> whose <see cref="InboxMessage.NextRetryAtUtc"/>
        /// has elapsed, AND entries still in <see cref="InboxMessageStatus.Processing"/> whose
        /// <see cref="InboxMessage.LeaseExpiresAtUtc"/> has elapsed - an abandoned lease left behind by a
        /// worker that claimed the entry and then crashed or was killed before calling
        /// <see cref="CompleteAsync"/>/<see cref="FailAsync"/>/<see cref="DeadLetterAsync"/>, so it would
        /// otherwise never resurface (broker redelivery is not guaranteed, and this status never reaches
        /// <see cref="InboxMessageStatus.Failed"/> on its own). Does not itself claim anything - a
        /// returned entry may already be claimed by the time a caller's own <see cref="TryClaimAsync"/>
        /// runs, which must then report <see cref="InboxClaimOutcome.ClaimedByAnotherOwner"/> rather than
        /// double-processing it.
        /// </summary>
        Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes at most <see cref="InboxPurgeRequest.MaxCount"/> terminal
        /// (<see cref="InboxMessageStatus.Processed"/>/<see cref="InboxMessageStatus.DeadLettered"/>)
        /// entries matching <paramref name="request"/>. Must never remove a leased, retry-eligible, or
        /// excluded entry - a non-terminal entry (Received/Processing/Failed-awaiting-retry) is never
        /// eligible regardless of age, by construction (only <see cref="InboxMessageStatus.Processed"/>/
        /// <see cref="InboxMessageStatus.DeadLettered"/> are ever considered).
        /// </summary>
        Task<InboxPurgeResult> PurgeAsync(InboxPurgeRequest request, CancellationToken cancellationToken = default);

        /// <summary>
        /// Atomically transitions a terminal entry (<see cref="InboxMessageStatus.Processed"/> or
        /// <see cref="InboxMessageStatus.DeadLettered"/>) back to <see cref="InboxMessageStatus.Received"/>
        /// via <see cref="InboxMessage.Replay"/> - the manual replay/requeue operation
        /// <see cref="InboxDeadLetterReplayService"/> builds on. Idempotent in the sense that replaying
        /// an entry that is no longer terminal (e.g. a concurrent replay already moved it, or it was
        /// independently reclaimed) returns <see langword="null"/> rather than applying a second replay -
        /// implementations must verify the entry is still terminal as part of the same atomic operation
        /// that applies the transition, the same compare-and-swap discipline every lease-scoped mutation
        /// here already follows. Returns <see langword="null"/> if the entry does not exist or is not
        /// currently terminal.
        /// </summary>
        Task<InboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default);
    }
}
