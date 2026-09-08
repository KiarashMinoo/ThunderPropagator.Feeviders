using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// A minimal, correctness-first (not performance-optimized) <see cref="IOutboxStore"/> that exists
    /// only to prove <see cref="OutboxStoreContractTests"/> actually enforces the guarantees it claims
    /// to. A real backend (the InMemory/Redis/EFCore/MongoDB store issues that depend on #109/#116)
    /// supplies its own <see cref="IOutboxStore"/> and its own <see cref="OutboxStoreContractTests"/>
    /// subclass instead of this one.
    /// </summary>
    internal sealed class ReferenceOutboxStore(TimeProvider timeProvider) : IOutboxStore
    {
        // OutboxMessage.PartitionKey is nullable; this dictionary key must not be, so the null
        // partition is tracked under a sentinel that can never collide with a real partition key
        // (a real key is caller-supplied text, never this exact null-marker string).
        private const string NullPartitionSentinel = "\0null-partition\0";

        private readonly object _gate = new();
        private readonly Dictionary<Guid, OutboxMessage> _byId = [];
        private readonly Dictionary<string, long> _nextOrderingSequenceByPartition = [];

        /// <summary>Test-only inspection, bypassing every <see cref="IOutboxStore"/> claim/lease semantic - never used by production code.</summary>
        internal OutboxMessage Peek(string messageId)
        {
            lock (_gate)
                return _byId.Values.Single(m => m.MessageId == messageId);
        }

        /// <summary>Test-only inspection - <see langword="null"/> if <paramref name="messageId"/> does not exist (e.g. it was purged), unlike <see cref="Peek"/>.</summary>
        internal OutboxMessage? TryPeek(string messageId)
        {
            lock (_gate)
                return _byId.Values.SingleOrDefault(m => m.MessageId == messageId);
        }

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
                    request.Headers, request.PartitionKey, timeProvider);

                _byId[message.Id] = message;
                return Task.FromResult(message);
            }
        }

        public Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string? partitionKey, int maxCount, string leaseOwner, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var now = timeProvider.GetUtcNow();
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
                        ? candidate.TryTransitionTo(OutboxMessageStatus.Failed, timeProvider)
                        : candidate;

                    var publishing = source.TryTransitionTo(
                        OutboxMessageStatus.Publishing, timeProvider,
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

        public Task<IReadOnlyList<string?>> GetClaimablePartitionKeysAsync(CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var now = timeProvider.GetUtcNow();
                IReadOnlyList<string?> partitionKeys = [.. _byId.Values.Where(m => IsClaimable(m, now)).Select(m => m.PartitionKey).Distinct()];
                return Task.FromResult(partitionKeys);
            }
        }

        public Task<OutboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing))
                    return Task.FromResult<OutboxMessage?>(null);

                var renewed = existing.RenewLease(timeProvider, leaseOwner, leaseExtension);
                if (renewed is not null)
                    _byId[id] = renewed;

                return Task.FromResult(renewed);
            }
        }

        public Task<OutboxMessage?> MarkPublishedAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Published, timeProvider)));

        public Task<OutboxMessage?> MarkFailedAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.Failed, timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason)));

        public Task<OutboxMessage?> MarkDeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(OutboxMessageStatus.DeadLettered, timeProvider, failureReason: failureReason)));

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

        public Task<int> GetDepthAsync(string? partitionKey, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                return Task.FromResult(_byId.Values.Count(m => MatchesScope(m, partitionKey) && IsBacklog(m.Status)));
            }
        }

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

        public Task<OutboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing) || existing.Status is not (OutboxMessageStatus.Published or OutboxMessageStatus.DeadLettered))
                    return Task.FromResult<OutboxMessage?>(null);

                var partitionSlot = PartitionSlot(existing.PartitionKey);
                var newOrderingSequence = _nextOrderingSequenceByPartition.GetValueOrDefault(partitionSlot, 0);
                _nextOrderingSequenceByPartition[partitionSlot] = newOrderingSequence + 1;

                var requeued = existing.Requeue(newOrderingSequence, timeProvider);
                _byId[id] = requeued;
                return Task.FromResult<OutboxMessage?>(requeued);
            }
        }

        private static string PartitionSlot(string? partitionKey) => partitionKey ?? NullPartitionSentinel;
    }
}
