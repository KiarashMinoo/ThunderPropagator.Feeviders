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
        /// Returns entries in <see cref="InboxMessageStatus.Failed"/> whose
        /// <see cref="InboxMessage.NextRetryAtUtc"/> has elapsed, for a retry worker to claim via
        /// <see cref="TryClaimAsync"/>. Does not itself claim anything.
        /// </summary>
        Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes terminal (<see cref="InboxMessageStatus.Processed"/>/<see cref="InboxMessageStatus.DeadLettered"/>)
        /// entries older than <paramref name="olderThanUtc"/>. Must never remove a leased or
        /// retry-eligible entry. Returns the number of entries purged.
        /// </summary>
        Task<int> PurgeAsync(Guid channelKey, DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default);
    }
}
