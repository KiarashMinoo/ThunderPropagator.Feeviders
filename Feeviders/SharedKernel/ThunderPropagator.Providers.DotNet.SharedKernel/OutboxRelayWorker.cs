using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    /// <summary>
    /// Polls every registered <see cref="OutboxRelaySubscription"/> for claimable entries (Pending, or
    /// Publishing/Failed reclaimable per <see cref="IOutboxStore.ClaimBatchAsync"/> - an abandoned or
    /// expired-retry lease) and relays them to the broker: discovers claimable partitions via
    /// <see cref="IOutboxStore.GetClaimablePartitionKeysAsync"/>, claims one batch per partition, and
    /// publishes each claimed entry, in order, through the subscription's <see cref="IProvider"/> bypass
    /// (<see cref="IProvider.PublishDirectAsync(byte[], IReadOnlyDictionary{string, string}?, CancellationToken)"/>) -
    /// never through the normal enqueue-deciding publish path, which would recurse back into the Outbox.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Different partitions are claimed and published concurrently (bounded by the configured degree of
    /// parallelism), always independently - one partition's backlog, failure, or backoff never delays
    /// another's. Entries within one partition are always published strictly in
    /// <see cref="OutboxMessage.OrderingSequence"/> order, one at a time. If publishing entry N in a
    /// partition's batch fails transiently (backed off for retry, not dead-lettered), the default
    /// <see cref="OutboxOrderingPolicy.StrictPerPartition"/> releases every later entry already claimed
    /// in that same batch back to <see cref="OutboxMessageStatus.Pending"/> (via
    /// <see cref="IOutboxStore.ReleaseAsync"/>) rather than publishing them - publishing them now would
    /// put them ahead of N, which has not published yet; <see cref="OutboxOrderingPolicy.ContinueOnFailure"/>
    /// instead lets them publish immediately, trading that ordering guarantee for availability. A
    /// dead-lettered entry, in contrast, will never publish at all under either policy, so it is skipped
    /// without releasing the rest of the batch: the sequence of entries that do eventually publish stays
    /// in order regardless of policy.
    /// </para>
    /// <para>
    /// Every publish attempt carries <see cref="OutboxHeaderNames.MessageId"/>, stamped with the entry's
    /// stable <see cref="OutboxMessage.MessageId"/> - see <see cref="OutboxOptions"/> remarks on
    /// downstream deduplication. A crash between the broker's acknowledgement and this worker's
    /// <see cref="IOutboxStore.MarkPublishedAsync"/> call leaves the entry Publishing until its lease
    /// expires, after which it becomes claimable again and is republished under that same
    /// <see cref="OutboxMessage.MessageId"/> - the documented at-least-once (never exactly-once, never
    /// duplicate-free) delivery guarantee this worker provides.
    /// </para>
    /// <para>
    /// Hosting-agnostic by design (plain <see cref="StartAsync"/>/<see cref="StopAsync"/>, not
    /// <c>IHostedService</c>), mirroring <c>InboxRetryWorker</c>. <see cref="RunOnceAsync"/> processes one
    /// batch per claimable partition per subscription directly, without waiting on a polling interval -
    /// the entry point tests use to drive relaying deterministically against a fake clock.
    /// </para>
    /// </remarks>
    public sealed class OutboxRelayWorker : IAsyncDisposable
    {
        private readonly IReadOnlyList<ResolvedSubscription> _subscriptions;
        private readonly IServiceProvider _serviceProvider;
        private readonly TimeProvider _timeProvider;
        private readonly Random _random;
        private readonly int _maxDegreeOfParallelism;
        private CancellationTokenSource? _stoppingCts;
        private Task[]? _pollingLoops;

        /// <param name="subscriptions">One entry per Provider to relay. A subscription with <see cref="OutboxOptions.OutboxEnabled"/> false is skipped entirely - it does not even resolve a store.</param>
        /// <param name="storeFactory">Resolves each enabled subscription's store once, here at construction - not once per poll.</param>
        /// <param name="serviceProvider">Passed to <see cref="OutboxRelaySubscription.ResolveProvider"/>/<see cref="OutboxRelaySubscription.CreateDeadLetterHandler"/> on every use, since either may itself be scoped.</param>
        /// <param name="timeProvider">Clock used for lease/backoff arithmetic and to drive the polling timer. Defaults to <see cref="TimeProvider.System"/> - tests should supply a fake for deterministic backoff/lease-expiry assertions.</param>
        /// <param name="random">Jitter source for <see cref="OutboxRetryBackoff"/>. Defaults to <see cref="Random.Shared"/> - tests should supply a seeded instance for deterministic backoff assertions.</param>
        /// <param name="maxDegreeOfParallelism">Upper bound on concurrently in-flight partition claims within one subscription's poll.</param>
        /// <exception cref="ArgumentException">Two subscriptions share the same <see cref="OutboxRelaySubscription.ProviderKey"/>, or an enabled subscription's options fail <see cref="OutboxOptions.Validate"/>.</exception>
        /// <exception cref="InvalidOperationException">An enabled subscription has no <see cref="OutboxOptions.StoreConnectionName"/>.</exception>
        public OutboxRelayWorker(
            IEnumerable<OutboxRelaySubscription> subscriptions,
            IOutboxStoreFactory storeFactory,
            IServiceProvider serviceProvider,
            TimeProvider? timeProvider = null,
            Random? random = null,
            int maxDegreeOfParallelism = 4)
        {
            ArgumentNullException.ThrowIfNull(subscriptions);
            ArgumentNullException.ThrowIfNull(storeFactory);
            ArgumentNullException.ThrowIfNull(serviceProvider);

            if (maxDegreeOfParallelism < 1)
                throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism), maxDegreeOfParallelism, "Must be at least 1.");

            _serviceProvider = serviceProvider;
            _timeProvider = timeProvider ?? TimeProvider.System;
            _random = random ?? Random.Shared;
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
            _subscriptions = ResolveSubscriptions(subscriptions, storeFactory);
        }

        private static IReadOnlyList<ResolvedSubscription> ResolveSubscriptions(IEnumerable<OutboxRelaySubscription> subscriptions, IOutboxStoreFactory storeFactory)
        {
            var seenProviders = new HashSet<string>(StringComparer.Ordinal);
            var resolved = new List<ResolvedSubscription>();

            foreach (var subscription in subscriptions)
            {
                if (!seenProviders.Add(subscription.ProviderKey))
                    throw new ArgumentException($"Provider '{subscription.ProviderKey}' has more than one Outbox relay subscription.", nameof(subscriptions));

                if (!subscription.Options.OutboxEnabled)
                    continue;

                subscription.Options.Validate();

                if (string.IsNullOrWhiteSpace(subscription.Options.StoreConnectionName))
                    throw new InvalidOperationException(
                        $"{nameof(OutboxOptions.StoreConnectionName)} must be set to resolve a registered Outbox store for provider '{subscription.ProviderKey}'.");

                var store = storeFactory.GetStore(subscription.Options.StoreConnectionName, subscription.Options.StoreType);
                resolved.Add(new ResolvedSubscription(subscription, store));
            }

            return resolved;
        }

        /// <summary>Processes at most one batch per claimable partition, per subscription, and returns - does not loop or wait on a polling interval.</summary>
        public async Task RunOnceAsync(CancellationToken cancellationToken = default)
        {
            foreach (var resolved in _subscriptions)
                await ProcessSubscriptionAsync(resolved, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Starts one polling loop per enabled subscription, each on its own <see cref="OutboxOptions.RelayPollingInterval"/>.</summary>
        /// <exception cref="InvalidOperationException">Already started.</exception>
        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_stoppingCts is not null)
                throw new InvalidOperationException($"{nameof(OutboxRelayWorker)} is already started.");

            _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _pollingLoops = [.. _subscriptions.Select(resolved => PollAsync(resolved, _stoppingCts.Token))];
            return Task.CompletedTask;
        }

        private async Task PollAsync(ResolvedSubscription resolved, CancellationToken cancellationToken)
        {
            using var timer = new PeriodicTimer(resolved.Subscription.Options.RelayPollingInterval, _timeProvider);

            try
            {
                while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                    await ProcessSubscriptionAsync(resolved, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on graceful stop.
            }
        }

        /// <summary>
        /// Cancels every polling loop (no new batch starts) and waits, up to <paramref name="drainTimeout"/>,
        /// for whatever batch each loop is currently mid-processing to finish naturally. An entry a batch
        /// does not finish in time simply keeps its lease until it expires - the next run then reclaims
        /// it as an abandoned lease, so shutdown never strands an entry unrecoverable, only delayed.
        /// </summary>
        public async Task StopAsync(TimeSpan? drainTimeout = null, CancellationToken cancellationToken = default)
        {
            var stoppingCts = Interlocked.Exchange(ref _stoppingCts, null);
            if (stoppingCts is null)
                return;

            await stoppingCts.CancelAsync().ConfigureAwait(false);

            var loops = Interlocked.Exchange(ref _pollingLoops, null);
            if (loops is { Length: > 0 })
            {
                try
                {
                    await Task.WhenAll(loops).WaitAsync(drainTimeout ?? Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    // Best-effort drain - see the lease-expiry recovery note above.
                }
            }

            stoppingCts.Dispose();
        }

        private async Task ProcessSubscriptionAsync(ResolvedSubscription resolved, CancellationToken cancellationToken)
        {
            var partitionKeys = await resolved.Store.GetClaimablePartitionKeysAsync(cancellationToken).ConfigureAwait(false);
            if (partitionKeys.Count == 0)
                return;

            using var throttle = new SemaphoreSlim(_maxDegreeOfParallelism);
            var loops = partitionKeys.Select(partitionKey => ProcessPartitionAsync(resolved, partitionKey, throttle, cancellationToken));
            await Task.WhenAll(loops).ConfigureAwait(false);
        }

        private async Task ProcessPartitionAsync(ResolvedSubscription resolved, string? partitionKey, SemaphoreSlim throttle, CancellationToken cancellationToken)
        {
            await throttle.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var (subscription, store) = resolved;
                var leaseOwner = Guid.NewGuid().ToString("N");

                var batch = await store.ClaimBatchAsync(partitionKey, subscription.Options.RelayBatchSize, leaseOwner, subscription.Options.ClaimLeaseDuration, cancellationToken)
                    .ConfigureAwait(false);

                for (var i = 0; i < batch.Count; i++)
                {
                    var outcome = await PublishOneAsync(resolved, batch[i], leaseOwner, cancellationToken).ConfigureAwait(false);
                    if (outcome != RelayOutcome.FailedRetryable || subscription.Options.OrderingPolicy == OutboxOrderingPolicy.ContinueOnFailure)
                        continue;

                    // Every later entry in this batch was already claimed alongside the one that just
                    // failed - releasing them (instead of publishing them now) keeps this partition's
                    // published order matching its enqueue order once the failed entry is retried.
                    for (var j = i + 1; j < batch.Count; j++)
                        await store.ReleaseAsync(batch[j].Id, leaseOwner, cancellationToken).ConfigureAwait(false);

                    break;
                }
            }
            finally
            {
                throttle.Release();
            }
        }

        private async Task<RelayOutcome> PublishOneAsync(ResolvedSubscription resolved, OutboxMessage claimed, string leaseOwner, CancellationToken cancellationToken)
        {
            var (subscription, store) = resolved;

            try
            {
                var provider = subscription.ResolveProvider(_serviceProvider);
                await provider.PublishDirectAsync(claimed.Payload, WithMessageIdHeader(claimed), cancellationToken).ConfigureAwait(false);
                await store.MarkPublishedAsync(claimed.Id, leaseOwner, cancellationToken).ConfigureAwait(false);
                return RelayOutcome.Published;
            }
            catch (OutboxNonRetryableException exception)
            {
                await DeadLetterAsync(resolved, claimed, leaseOwner, exception.Message, cancellationToken).ConfigureAwait(false);
                return RelayOutcome.DeadLettered;
            }
            catch (Exception exception)
            {
                var reason = SanitizeFailureReason(exception);

                if (claimed.Attempts >= subscription.Options.MaxRetryAttempts)
                {
                    await DeadLetterAsync(resolved, claimed, leaseOwner, reason, cancellationToken).ConfigureAwait(false);
                    return RelayOutcome.DeadLettered;
                }

                var nextRetryAtUtc = _timeProvider.GetUtcNow() + OutboxRetryBackoff.Compute(subscription.Options, claimed.Attempts, _random);
                await store.MarkFailedAsync(claimed.Id, leaseOwner, reason, nextRetryAtUtc, cancellationToken).ConfigureAwait(false);
                return RelayOutcome.FailedRetryable;
            }
        }

        /// <summary>
        /// Stamps <see cref="OutboxHeaderNames.MessageId"/> with <see cref="OutboxMessage.MessageId"/> on
        /// every publish attempt, overriding any value already present under that key - a downstream
        /// consumer's deduplication key must always be this Outbox's own stable identifier, never
        /// whatever the enqueuing call site happened to put there.
        /// </summary>
        private static IReadOnlyDictionary<string, string> WithMessageIdHeader(OutboxMessage claimed)
        {
            var headers = new Dictionary<string, string>(claimed.Headers) { [OutboxHeaderNames.MessageId] = claimed.MessageId };
            return headers;
        }

        private async Task DeadLetterAsync(ResolvedSubscription resolved, OutboxMessage claimed, string leaseOwner, string reason, CancellationToken cancellationToken)
        {
            var deadLettered = await resolved.Store.MarkDeadLetterAsync(claimed.Id, leaseOwner, reason, cancellationToken).ConfigureAwait(false);

            if (deadLettered is null || resolved.Subscription.CreateDeadLetterHandler is null)
                return;

            try
            {
                var notifier = resolved.Subscription.CreateDeadLetterHandler(_serviceProvider);
                await notifier.HandleAsync(deadLettered, reason, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // A failing notification must not undo or retry the dead-lettering that already
                // durably succeeded above - the (future) dead-letter pipeline's own concern to recover.
            }
        }

        /// <summary>
        /// Never persists <see cref="Exception.Message"/> as-is (it may embed payload content or other
        /// sensitive detail) - only the exception's type name, mirroring <c>InboxRetryWorker</c>'s same
        /// rule.
        /// </summary>
        private static string SanitizeFailureReason(Exception exception)
        {
            var reason = exception.GetType().FullName ?? exception.GetType().Name;
            return reason.Length > OutboxMessageLimits.MaxFailureReasonLength
                ? reason[..OutboxMessageLimits.MaxFailureReasonLength]
                : reason;
        }

        public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

        private enum RelayOutcome
        {
            Published,
            FailedRetryable,
            DeadLettered,
        }

        private sealed record ResolvedSubscription(OutboxRelaySubscription Subscription, IOutboxStore Store);
    }
}
