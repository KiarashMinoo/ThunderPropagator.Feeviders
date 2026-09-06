using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// A minimal, correctness-first (not performance-optimized) <see cref="IInboxStore"/> that exists
    /// only to prove <see cref="InboxStoreContractTests"/> actually enforces the guarantees it claims
    /// to. A real backend (the InMemory/Redis/EFCore/MongoDB store issues that depend on #109) supplies
    /// its own <see cref="IInboxStore"/> and its own <see cref="InboxStoreContractTests"/> subclass
    /// instead of this one.
    /// </summary>
    internal sealed class ReferenceInboxStore(TimeProvider timeProvider) : IInboxStore
    {
        private readonly object _gate = new();
        private readonly Dictionary<Guid, InboxMessage> _byId = [];
        private readonly Dictionary<(Guid ChannelKey, string? PartitionKey, string MessageId), Guid> _dedupIndex = [];

        public Task<InboxClaimResult> TryClaimAsync(InboxClaimRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_gate)
            {
                var key = (request.ChannelKey, request.PartitionKey, request.MessageId);
                var now = timeProvider.GetUtcNow();

                if (_dedupIndex.TryGetValue(key, out var existingId))
                {
                    var existing = _byId[existingId];
                    var retryEligible = existing.NextRetryAtUtc is null || existing.NextRetryAtUtc <= now;
                    var leaseExpired = existing.LeaseExpiresAtUtc is null || existing.LeaseExpiresAtUtc <= now;

                    var result = existing.Status switch
                    {
                        InboxMessageStatus.Processed => InboxClaimResult.AlreadyProcessed(existing),
                        InboxMessageStatus.DeadLettered => InboxClaimResult.DeadLettered(existing),
                        InboxMessageStatus.Processing when !leaseExpired => InboxClaimResult.ClaimedByAnotherOwner(existing),
                        InboxMessageStatus.Failed when !retryEligible => InboxClaimResult.ClaimedByAnotherOwner(existing),
                        _ => InboxClaimResult.Claimed(Reclaim(existing, request, now)),
                    };

                    if (result.Outcome == InboxClaimOutcome.Claimed)
                        _byId[result.Message!.Id] = result.Message;

                    return Task.FromResult(result);
                }

                var received = InboxMessage.CreateReceived(
                    Guid.NewGuid(), request.MessageId, request.ChannelKey, request.FeederId,
                    request.SchemaVersion, request.PayloadContentType, request.Payload,
                    request.Headers, request.PartitionKey, timeProvider);
                var claimed = received.TryTransitionTo(
                    InboxMessageStatus.Processing, timeProvider,
                    leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

                _byId[claimed.Id] = claimed;
                _dedupIndex[key] = claimed.Id;
                return Task.FromResult(InboxClaimResult.Claimed(claimed));
            }
        }

        // A lapsed Processing lease must first release back through Failed (Processing isn't a valid
        // source for Processing in InboxMessage's transition table) before it can be reclaimed.
        private InboxMessage Reclaim(InboxMessage existing, InboxClaimRequest request, DateTimeOffset now) =>
            (existing.Status == InboxMessageStatus.Processing ? existing.TryTransitionTo(InboxMessageStatus.Failed, timeProvider) : existing)
            .TryTransitionTo(InboxMessageStatus.Processing, timeProvider, leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

        public Task<InboxMessage?> GetAsync(string messageId, Guid channelKey, string? partitionKey, CancellationToken cancellationToken = default)
        {
            lock (_gate)
                return Task.FromResult(_dedupIndex.TryGetValue((channelKey, partitionKey, messageId), out var id) ? _byId[id] : null);
        }

        public Task<InboxMessage?> CompleteAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Processed, timeProvider)));

        public Task<InboxMessage?> FailAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Failed, timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason)));

        public Task<InboxMessage?> DeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.DeadLettered, timeProvider, failureReason: failureReason)));

        public Task<InboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing))
                    return Task.FromResult<InboxMessage?>(null);

                var renewed = existing.RenewLease(timeProvider, leaseOwner, leaseExtension);
                if (renewed is not null)
                    _byId[id] = renewed;

                return Task.FromResult(renewed);
            }
        }

        public Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var now = timeProvider.GetUtcNow();
                IReadOnlyList<InboxMessage> retryable = _byId.Values
                    .Where(m => m.ChannelKey == channelKey && m.Status == InboxMessageStatus.Failed && (m.NextRetryAtUtc is null || m.NextRetryAtUtc <= now))
                    .Take(maxCount)
                    .ToArray();

                return Task.FromResult(retryable);
            }
        }

        public Task<int> PurgeAsync(Guid channelKey, DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var toPurge = _byId.Values
                    .Where(m => m.ChannelKey == channelKey)
                    .Where(m => m.Status switch
                    {
                        InboxMessageStatus.Processed => m.ProcessedAtUtc < olderThanUtc,
                        InboxMessageStatus.DeadLettered => m.DeadLetteredAtUtc < olderThanUtc,
                        _ => false,
                    })
                    .ToArray();

                foreach (var message in toPurge)
                {
                    _byId.Remove(message.Id);
                    _dedupIndex.Remove((message.ChannelKey, message.PartitionKey, message.MessageId));
                }

                return Task.FromResult(toPurge.Length);
            }
        }

        private InboxMessage? TransitionLeased(Guid id, string leaseOwner, Func<InboxMessage, InboxMessage> transition)
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
    }
}
