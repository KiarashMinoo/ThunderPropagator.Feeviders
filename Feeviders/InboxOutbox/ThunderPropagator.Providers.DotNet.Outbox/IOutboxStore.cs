namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Atomic, backend-independent Outbox persistence contract. Every method returns an immutable
    /// <see cref="OutboxMessage"/> snapshot (or <see langword="null"/> where the operation could
    /// not be applied) rather than mutating a shared instance.
    /// </summary>
    /// <remarks>
    /// Replaces a plain <c>GetPendingAsync</c> + status-update pattern - which lets multiple relay
    /// workers publish the same row concurrently and defines no ordering - with claim-token-based
    /// batch claims scoped to a partition. Implementations must guarantee: two concurrent relay
    /// workers can never both claim the same entry; entries within one partition are claimed in
    /// <see cref="OutboxMessage.OrderingSequence"/> order; different partitions may be claimed and
    /// published concurrently and independently; and an abandoned (lease-expired) <see cref="OutboxMessageStatus.Publishing"/>
    /// entry is recoverable by a later claim. Where a supported unit of work exists,
    /// <see cref="EnqueueAsync"/> is expected to be called within the same local transaction as the
    /// business state it accompanies - that enlistment boundary is defined by the caller
    /// (typically <c>AbstractProvider</c>'s opt-in enqueue path), not by this contract itself.
    /// </remarks>
    public interface IOutboxStore
    {
        /// <summary>Durably enqueues a new message in <see cref="OutboxMessageStatus.Pending"/>.</summary>
        Task<OutboxMessage> EnqueueAsync(OutboxEnqueueRequest request, CancellationToken cancellationToken = default);

        /// <summary>
        /// Atomically claims up to <paramref name="maxCount"/> <see cref="OutboxMessageStatus.Pending"/>
        /// (or lease-expired <see cref="OutboxMessageStatus.Publishing"/>) entries within
        /// <paramref name="partitionKey"/>, in <see cref="OutboxMessage.OrderingSequence"/> order,
        /// stamping each with <paramref name="leaseOwner"/> and moving it to
        /// <see cref="OutboxMessageStatus.Publishing"/>. No other caller can claim the same entries
        /// until the lease expires.
        /// </summary>
        Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string? partitionKey, int maxCount, string leaseOwner, TimeSpan leaseDuration, CancellationToken cancellationToken = default);

        /// <summary>
        /// Extends an active lease's expiry, for slow publish operations. Returns
        /// <see langword="null"/> if <paramref name="leaseOwner"/> no longer matches the current lease.
        /// </summary>
        Task<OutboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default);

        /// <summary>
        /// Marks a leased entry <see cref="OutboxMessageStatus.Published"/>. Callers must only call
        /// this after the broker adapter's own acknowledgement condition has been met. Returns
        /// <see langword="null"/> if <paramref name="leaseOwner"/> no longer matches the current lease.
        /// </summary>
        Task<OutboxMessage?> MarkPublishedAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default);

        /// <summary>
        /// Marks a leased entry <see cref="OutboxMessageStatus.Failed"/> and releases the lease, so a
        /// relay worker can claim it again at/after <paramref name="nextRetryAtUtc"/>. Returns
        /// <see langword="null"/> if <paramref name="leaseOwner"/> no longer matches the current lease.
        /// </summary>
        Task<OutboxMessage?> MarkFailedAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default);

        /// <summary>
        /// Marks a leased entry <see cref="OutboxMessageStatus.DeadLettered"/> (retry policy
        /// exhausted, or the failure was flagged non-retryable). Returns <see langword="null"/> if
        /// <paramref name="leaseOwner"/> no longer matches the current lease.
        /// </summary>
        Task<OutboxMessage?> MarkDeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default);

        /// <summary>
        /// Releases a lease back to <see cref="OutboxMessageStatus.Pending"/> without recording a
        /// failure (e.g. on graceful relay-worker shutdown), preserving <see cref="OutboxMessage.OrderingSequence"/>.
        /// Returns <see langword="false"/> if <paramref name="leaseOwner"/> no longer matches the current lease.
        /// </summary>
        Task<bool> ReleaseAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default);

        /// <summary>Number of not-yet-<see cref="OutboxMessageStatus.Published"/> entries, optionally scoped to a partition.</summary>
        Task<int> GetDepthAsync(string? partitionKey, CancellationToken cancellationToken = default);

        /// <summary>
        /// Age of the oldest not-yet-<see cref="OutboxMessageStatus.Published"/> entry, optionally
        /// scoped to a partition, or <see langword="null"/> if there is none.
        /// </summary>
        Task<TimeSpan?> GetOldestPendingAgeAsync(string? partitionKey, TimeProvider timeProvider, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes terminal (<see cref="OutboxMessageStatus.Published"/>/<see cref="OutboxMessageStatus.DeadLettered"/>)
        /// entries older than <paramref name="olderThanUtc"/>. Must never remove a leased or
        /// retry-eligible entry. Returns the number of entries purged.
        /// </summary>
        Task<int> PurgeAsync(DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default);
    }
}
