namespace ThunderPropagator.Feeders.Inbox.InMemory
{
    /// <summary>
    /// Process-local, non-durable <see cref="IInboxStore"/> backed by a lock-guarded dictionary. Every
    /// entry lives only in this process's memory - a restart or crash loses everything, and two
    /// processes each construct their own independent store even if registered under the same
    /// <see cref="InboxOptions.StoreConnectionName"/>. Not appropriate for any deployment where
    /// <see cref="InboxOptions.RequireDurableStore"/> would matter; intended for local development,
    /// single-process testing, and any Feevider that has genuinely opted out of durability.
    /// </summary>
    /// <remarks>
    /// Every operation holds one internal lock for its whole duration - correct under concurrent access
    /// (the same guarantee <see cref="IInboxStore"/> requires of every backend), not throughput-optimized.
    /// Construct one instance per isolated store: nothing here is shared process-wide by default, so
    /// tests get full isolation for free by simply constructing a fresh instance instead of needing to
    /// reset or clear one.
    /// </remarks>
    public sealed class InMemoryInboxStore(TimeProvider? timeProvider = null) : IInboxStore
    {
        private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
        private readonly object _gate = new();
        private readonly Dictionary<Guid, InboxMessage> _byId = [];
        private readonly Dictionary<(Guid ChannelKey, string? PartitionKey, string MessageId), Guid> _dedupIndex = [];

        /// <inheritdoc/>
        public Task<InboxClaimResult> TryClaimAsync(InboxClaimRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_gate)
            {
                var key = (request.ChannelKey, request.PartitionKey, request.MessageId);
                var now = _timeProvider.GetUtcNow();

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
                    request.Headers, request.PartitionKey, _timeProvider);
                var claimed = received.TryTransitionTo(
                    InboxMessageStatus.Processing, _timeProvider,
                    leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

                _byId[claimed.Id] = claimed;
                _dedupIndex[key] = claimed.Id;
                return Task.FromResult(InboxClaimResult.Claimed(claimed));
            }
        }

        // A lapsed Processing lease must first release back through Failed (Processing isn't a valid
        // source for Processing in InboxMessage's transition table) before it can be reclaimed.
        private InboxMessage Reclaim(InboxMessage existing, InboxClaimRequest request, DateTimeOffset now) =>
            (existing.Status == InboxMessageStatus.Processing ? existing.TryTransitionTo(InboxMessageStatus.Failed, _timeProvider) : existing)
            .TryTransitionTo(InboxMessageStatus.Processing, _timeProvider, leaseOwner: request.LeaseOwner, leaseExpiresAtUtc: now + request.LeaseDuration, incrementAttempt: true);

        /// <inheritdoc/>
        public Task<InboxMessage?> GetAsync(string messageId, Guid channelKey, string? partitionKey, CancellationToken cancellationToken = default)
        {
            lock (_gate)
                return Task.FromResult(_dedupIndex.TryGetValue((channelKey, partitionKey, messageId), out var id) ? _byId[id] : null);
        }

        /// <inheritdoc/>
        public Task<InboxMessage?> CompleteAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Processed, _timeProvider)));

        /// <inheritdoc/>
        public Task<InboxMessage?> FailAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.Failed, _timeProvider, nextRetryAtUtc: nextRetryAtUtc, failureReason: failureReason)));

        /// <inheritdoc/>
        public Task<InboxMessage?> DeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransitionLeased(id, leaseOwner, m => m.TryTransitionTo(InboxMessageStatus.DeadLettered, _timeProvider, failureReason: failureReason)));

        /// <inheritdoc/>
        public Task<InboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (!_byId.TryGetValue(id, out var existing))
                    return Task.FromResult<InboxMessage?>(null);

                var renewed = existing.RenewLease(_timeProvider, leaseOwner, leaseExtension);
                if (renewed is not null)
                    _byId[id] = renewed;

                return Task.FromResult(renewed);
            }
        }

        /// <inheritdoc/>
        public Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var now = _timeProvider.GetUtcNow();
                IReadOnlyList<InboxMessage> retryable = _byId.Values
                    .Where(m => m.ChannelKey == channelKey && IsRetryable(m, now))
                    .Take(maxCount)
                    .ToArray();

                return Task.FromResult(retryable);
            }
        }

        private static bool IsRetryable(InboxMessage message, DateTimeOffset now) =>
            message.Status switch
            {
                InboxMessageStatus.Failed => message.NextRetryAtUtc is null || message.NextRetryAtUtc <= now,
                InboxMessageStatus.Processing => message.LeaseExpiresAtUtc is null || message.LeaseExpiresAtUtc <= now,
                _ => false,
            };

        /// <inheritdoc/>
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
