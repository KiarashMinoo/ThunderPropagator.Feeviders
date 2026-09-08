namespace ThunderPropagator.Providers.DotNet.Outbox.InMemory
{
    /// <summary>
    /// Process-local, non-durable <see cref="IOutboxStore"/> backed by a lock-guarded dictionary. Every
    /// entry lives only in this process's memory - a restart or crash loses everything, and two
    /// processes each construct their own independent store even if registered under the same
    /// <see cref="OutboxOptions.StoreConnectionName"/>. Not appropriate for any deployment where
    /// <see cref="OutboxOptions.RequireDurableStore"/> would matter (<see cref="OutboxOptions.Validate"/>
    /// rejects that combination outright); intended for local development, single-process testing, and
    /// any Provider that has genuinely opted out of durability.
    /// </summary>
    /// <remarks>
    /// Every operation holds one internal lock for its whole duration - correct under concurrent access
    /// (the same guarantee <see cref="IOutboxStore"/> requires of every backend, including that two
    /// concurrent claims never double-own an entry and that a partition's entries claim in
    /// <see cref="OutboxMessage.OrderingSequence"/> order), not throughput-optimized. Construct one
    /// instance per isolated store: nothing here is shared process-wide by default, so tests get full
    /// isolation for free by simply constructing a fresh instance instead of needing to reset or clear
    /// one.
    /// </remarks>
    public sealed class InMemoryOutboxStore(TimeProvider? timeProvider = null) : IOutboxStore, IOutboxStoreInitializer
    {
        // OutboxMessage.PartitionKey is nullable; this dictionary key must not be, so the null
        // partition is tracked under a sentinel that can never collide with a real partition key
        // (a real key is caller-supplied text, never this exact null-marker string).
        private const string NullPartitionSentinel = "\0null-partition\0";

        private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
        private readonly object _gate = new();
        private readonly Dictionary<Guid, OutboxMessage> _byId = [];
        private readonly Dictionary<string, long> _nextOrderingSequenceByPartition = [];

        /// <summary>
        /// Trivially always ready: a process-local dictionary has no persisted schema to create/verify,
        /// and (per the class remarks) is never shared across replicas, so there is nothing to race
        /// over either. Implemented only so callers never need to special-case this backend - see
        /// <see cref="IOutboxStoreInitializer"/>'s remarks.
        /// </summary>
        public Task<StoreInitializationResult> InitializeAsync(StoreInitializationMode mode = StoreInitializationMode.Apply, CancellationToken cancellationToken = default) =>
            Task.FromResult(new StoreInitializationResult
            {
                Outcome = mode == StoreInitializationMode.VerifyOnly ? StoreInitializationOutcome.VerifiedCompatible : StoreInitializationOutcome.Ready,
                RequiredSchemaVersion = 1,
                PersistedSchemaVersion = 1,
                Message = "In-memory store has no persisted schema.",
            });

        /// <inheritdoc/>
        public Task<OutboxMessage> EnqueueAsync(OutboxEnqueueRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_gate)
            {
                var partitionSlot = PartitionSlot(request.PartitionKey);
                var orderingSequence = _nextOrderingSequenceByPartition.GetValueOrDefault(partitionSlot, 0);
                _nextOrderingSequenceByPartition[partitionSlot] = orderingSequence + 1;

                var message = OutboxMessage.CreatePending(
                    Guid.NewGuid(), request.MessageId, request.ProviderKey, orderingSequence,
                    request.SchemaVersion, request.PayloadContentType, request.Payload,
                    request.Headers, request.PartitionKey, _timeProvider);

                _byId[message.Id] = message;
                return Task.FromResult(message);
            }
        }

        /// <inheritdoc/>
        public Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string? partitionKey, int maxCount, string leaseOwner, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var now = _timeProvider.GetUtcNow();
                var claimed = new List<OutboxMessage>();

                foreach (var candidate in _byId.Values
                             .Where(m => m.PartitionKey == partitionKey && IsClaimable(m, now))
                             .OrderBy(m => m.OrderingSequence)
                             .Take(maxCount)
                             .ToArray())
                {
                    // An abandoned Publishing lease must first release back through Failed (Publishing
                    // isn't a valid source for Publishing in OutboxMessage's transition table) before it
                    // can be reclaimed. Every claim - first attempt, reclaimed abandoned lease, or
                    // retry - is itself a new attempt, so increments unconditionally.
                    var source = candidate.Status == OutboxMessageStatus.Publishing
                        ? candidate.TryTransitionTo(OutboxMessageStatus.Failed, _timeProvider)
                        : candidate;

                    var publishing = source.TryTransitionTo(
                        OutboxMessageStatus.Publishing, _timeProvider,
                        leaseOwner: leaseOwner, leaseExpiresAtUtc: now + leaseDuration, incrementAttempt: true);

                    _byId[publishing.Id] = publishing;
                    claimed.Add(publishing);
                }

                return Task.FromResult<IReadOnlyList<OutboxMessage>>(claimed);
            }
        }

        private static bool IsClaimable(OutboxMessage message, DateTimeOffset now) =>
            message.Status switch
            {
                OutboxMessageStatus.Pending => true,
                OutboxMessageStatus.Publishing => message.LeaseExpiresAtUtc is null || message.LeaseExpiresAtUtc <= now,
                OutboxMessageStatus.Failed => message.NextRetryAtUtc is null || message.NextRetryAtUtc <= now,
                _ => false,
            };

        /// <inheritdoc/>
        public Task<IReadOnlyList<string?>> GetClaimablePartitionKeysAsync(CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var now = _timeProvider.GetUtcNow();
                IReadOnlyList<string?> partitionKeys = [.. _byId.Values.Where(m => IsClaimable(m, now)).Select(m => m.PartitionKey).Distinct()];
                return Task.FromResult(partitionKeys);
            }
        }

        /// <inheritdoc/>
        public Task<OutboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing))
                    return Task.FromResult<OutboxMessage?>(null);

                var renewed = existing.RenewLease(_timeProvider, leaseOwner, leaseExtension);
                if (renewed is not null)
                    _byId[id] = renewed;

                return Task.FromResult(renewed);
            }
        }

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkPublishedAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Published, _timeProvider)));

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkFailedAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Failed, _timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason)));

        /// <inheritdoc/>
        public Task<OutboxMessage?> MarkDeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.DeadLettered, _timeProvider, failureReason: failureReason)));

        /// <inheritdoc/>
        public Task<bool> ReleaseAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing) || existing.LeaseOwner != leaseOwner || existing.Status != OutboxMessageStatus.Publishing)
                    return Task.FromResult(false);

                // Released, not failed - preserves OrderingSequence and does not record a failure, so
                // it goes straight back to Pending rather than through the normal transition table
                // (which has no Publishing -> Pending edge - that path is reserved for a genuine
                // failure/retry, not a graceful hand-back).
                _byId[id] = existing with { Status = OutboxMessageStatus.Pending, LeaseOwner = null, LeaseExpiresAtUtc = null, PublishingStartedAtUtc = null };
                return Task.FromResult(true);
            }
        }

        /// <inheritdoc/>
        public Task<int> GetDepthAsync(string? partitionKey, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                return Task.FromResult(_byId.Values.Count(m => MatchesScope(m, partitionKey) && IsBacklog(m.Status)));
            }
        }

        /// <inheritdoc/>
        public Task<TimeSpan?> GetOldestPendingAgeAsync(string? partitionKey, TimeProvider ageTimeProvider, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var oldest = _byId.Values
                    .Where(m => MatchesScope(m, partitionKey) && IsBacklog(m.Status))
                    .Select(m => m.CreatedAtUtc)
                    .OrderBy(t => t)
                    .Cast<DateTimeOffset?>()
                    .FirstOrDefault();

                return Task.FromResult(oldest is null ? null : (TimeSpan?)(ageTimeProvider.GetUtcNow() - oldest.Value));
            }
        }

        private static bool MatchesScope(OutboxMessage message, string? partitionKey) => partitionKey is null || message.PartitionKey == partitionKey;

        private static bool IsBacklog(OutboxMessageStatus status) => status is not (OutboxMessageStatus.Published or OutboxMessageStatus.DeadLettered);

        /// <inheritdoc/>
        public Task<OutboxPurgeResult> PurgeAsync(OutboxPurgeRequest request, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var eligible = _byId.Values
                    .Where(m => m.Status switch
                    {
                        OutboxMessageStatus.Published => request.PublishedOlderThanUtc is { } cutoff && m.PublishedAtUtc < cutoff,
                        OutboxMessageStatus.DeadLettered => request.DeadLetteredOlderThanUtc is { } cutoff && m.DeadLetteredAtUtc < cutoff,
                        _ => false,
                    })
                    .OrderBy(m => m.Status == OutboxMessageStatus.Published ? m.PublishedAtUtc : m.DeadLetteredAtUtc)
                    .Take(request.MaxCount + 1)
                    .ToArray();

                var hasMore = eligible.Length > request.MaxCount;
                var batch = hasMore ? eligible[..request.MaxCount] : eligible;
                var toPurge = request.ExcludedIds is null
                    ? batch
                    : [.. batch.Where(m => !request.ExcludedIds.Contains(m.Id))];

                foreach (var message in toPurge)
                    _byId.Remove(message.Id);

                return Task.FromResult(new OutboxPurgeResult { PurgedCount = toPurge.Length, HasMore = hasMore });
            }
        }

        private OutboxMessage? TransitionLeased(Guid id, string leaseOwner, Func<OutboxMessage, OutboxMessage> transition)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing) || existing.LeaseOwner != leaseOwner)
                    return null;

                var updated = transition(existing);
                _byId[id] = updated;
                return updated;
            }
        }

        /// <inheritdoc/>
        public Task<OutboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing) || existing.Status is not (OutboxMessageStatus.Published or OutboxMessageStatus.DeadLettered))
                    return Task.FromResult<OutboxMessage?>(null);

                var partitionSlot = PartitionSlot(existing.PartitionKey);
                var newOrderingSequence = _nextOrderingSequenceByPartition.GetValueOrDefault(partitionSlot, 0);
                _nextOrderingSequenceByPartition[partitionSlot] = newOrderingSequence + 1;

                var requeued = existing.Requeue(newOrderingSequence, _timeProvider);
                _byId[id] = requeued;
                return Task.FromResult<OutboxMessage?>(requeued);
            }
        }

        private static string PartitionSlot(string? partitionKey) => partitionKey ?? NullPartitionSentinel;
    }
}
