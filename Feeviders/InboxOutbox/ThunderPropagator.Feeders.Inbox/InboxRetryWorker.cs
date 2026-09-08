namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Polls every registered <see cref="InboxRetrySubscription"/> for retry-eligible entries (Failed
    /// past <see cref="InboxMessage.NextRetryAtUtc"/>, or Processing with an abandoned/expired lease -
    /// see <see cref="IInboxStore.QueryRetryableAsync"/>) and reattempts them: reclaims via
    /// <see cref="IInboxStore.TryClaimAsync"/> (the same atomic compare-and-swap a live receive path
    /// uses, so a worker here and a live redelivery of the same message can never both process it), then
    /// runs the channel's registered <see cref="IInboxRetryHandler"/> and completes/fails/dead-letters
    /// the claim based on the outcome.
    /// </summary>
    /// <remarks>
    /// Hosting-agnostic by design (plain <see cref="StartAsync"/>/<see cref="StopAsync"/>, not
    /// <c>IHostedService</c>) - a caller's own host wires these into whatever hosting model it uses, the
    /// same pattern <c>InboxReceiveCoordinator</c> follows. <see cref="RunOnceAsync"/> processes one
    /// batch per subscription directly, without waiting on a polling interval - the entry point tests use
    /// to drive retries deterministically against a fake clock.
    /// </remarks>
    public sealed class InboxRetryWorker : IAsyncDisposable
    {
        private readonly IReadOnlyList<ResolvedSubscription> _subscriptions;
        private readonly IServiceProvider _serviceProvider;
        private readonly TimeProvider _timeProvider;
        private readonly Random _random;
        private readonly int _maxDegreeOfParallelism;
        private CancellationTokenSource? _stoppingCts;
        private Task[]? _pollingLoops;

        /// <param name="subscriptions">
        /// One entry per channel to poll. A subscription with <see cref="InboxOptions.InboxEnabled"/>
        /// false is skipped entirely - it does not even resolve a store.
        /// </param>
        /// <param name="storeFactory">Resolves each enabled subscription's store once, here at construction - not once per poll.</param>
        /// <param name="serviceProvider">Passed to <see cref="InboxRetrySubscription.CreateHandler"/>/<see cref="InboxRetrySubscription.CreateDeadLetterHandlers"/> on every use, since a handler may itself be scoped.</param>
        /// <param name="timeProvider">Clock used for lease/backoff arithmetic and to drive the polling timer. Defaults to <see cref="TimeProvider.System"/> - tests should supply a fake for deterministic backoff/lease-expiry assertions.</param>
        /// <param name="random">Jitter source for <see cref="InboxRetryBackoff"/>. Defaults to <see cref="Random.Shared"/> - tests should supply a seeded instance for deterministic backoff assertions.</param>
        /// <param name="maxDegreeOfParallelism">Upper bound on concurrently in-flight reprocessing attempts within one subscription's batch.</param>
        /// <exception cref="ArgumentException">Two subscriptions share the same <see cref="InboxRetrySubscription.ChannelKey"/>, or an enabled subscription's options fail <see cref="InboxOptions.Validate"/>.</exception>
        /// <exception cref="InvalidOperationException">An enabled subscription has no <see cref="InboxOptions.StoreConnectionName"/>.</exception>
        public InboxRetryWorker(
            IEnumerable<InboxRetrySubscription> subscriptions,
            IInboxStoreFactory storeFactory,
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

        private static IReadOnlyList<ResolvedSubscription> ResolveSubscriptions(IEnumerable<InboxRetrySubscription> subscriptions, IInboxStoreFactory storeFactory)
        {
            var seenChannels = new HashSet<Guid>();
            var resolved = new List<ResolvedSubscription>();

            foreach (var subscription in subscriptions)
            {
                if (!seenChannels.Add(subscription.ChannelKey))
                    throw new ArgumentException($"Channel '{subscription.ChannelKey}' has more than one Inbox retry subscription.", nameof(subscriptions));

                if (!subscription.Options.InboxEnabled)
                    continue;

                subscription.Options.Validate();

                if (string.IsNullOrWhiteSpace(subscription.Options.StoreConnectionName))
                    throw new InvalidOperationException(
                        $"{nameof(InboxOptions.StoreConnectionName)} must be set to resolve a registered Inbox store for channel '{subscription.ChannelKey}'.");

                var store = storeFactory.GetStore(subscription.Options.StoreConnectionName, subscription.Options.StoreType);
                resolved.Add(new ResolvedSubscription(subscription, store));
            }

            return resolved;
        }

        /// <summary>Processes at most one batch (per subscription) of retry-eligible entries and returns - does not loop or wait on a polling interval.</summary>
        public async Task RunOnceAsync(CancellationToken cancellationToken = default)
        {
            foreach (var resolved in _subscriptions)
                await ProcessBatchAsync(resolved, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures every subscription's store is ready (see <see cref="IInboxStoreInitializer"/>), then
        /// starts one polling loop per enabled subscription, each on its own
        /// <see cref="InboxOptions.RetryPollingInterval"/>. No polling loop starts - not even for a
        /// subscription whose own store initialized fine - if any subscription's store is not ready.
        /// </summary>
        /// <exception cref="InvalidOperationException">Already started.</exception>
        /// <exception cref="InboxStoreInitializationFailedException">A subscription's store implements <see cref="IInboxStoreInitializer"/> and did not report <see cref="StoreInitializationResult.IsReady"/>.</exception>
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_stoppingCts is not null)
                throw new InvalidOperationException($"{nameof(InboxRetryWorker)} is already started.");

            foreach (var resolved in _subscriptions)
            {
                if (resolved.Store is not IInboxStoreInitializer initializer)
                    continue;

                var result = await initializer.InitializeAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                if (!result.IsReady)
                    throw new InboxStoreInitializationFailedException(resolved.Subscription.ChannelKey, result);
            }

            _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _pollingLoops = [.. _subscriptions.Select(resolved => PollAsync(resolved, _stoppingCts.Token))];
        }

        private async Task PollAsync(ResolvedSubscription resolved, CancellationToken cancellationToken)
        {
            using var timer = new PeriodicTimer(resolved.Subscription.Options.RetryPollingInterval, _timeProvider);

            try
            {
                while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                    await ProcessBatchAsync(resolved, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on graceful stop.
            }
        }

        /// <summary>
        /// Cancels every polling loop (no new batch starts) and waits, up to <paramref name="drainTimeout"/>,
        /// for whatever batch each loop is currently mid-processing to finish naturally. An entry a batch
        /// does not finish in time simply keeps its lease until it expires - <see cref="IInboxStore.QueryRetryableAsync"/>
        /// then surfaces it as an abandoned lease on the next run, so shutdown never strands an entry
        /// unrecoverable, only delayed.
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

        private async Task ProcessBatchAsync(ResolvedSubscription resolved, CancellationToken cancellationToken)
        {
            var candidates = await resolved.Store
                .QueryRetryableAsync(resolved.Subscription.ChannelKey, resolved.Subscription.Options.RetryBatchSize, cancellationToken)
                .ConfigureAwait(false);

            if (candidates.Count == 0)
                return;

            using var throttle = new SemaphoreSlim(_maxDegreeOfParallelism);
            var attempts = candidates.Select(candidate => ProcessOneAsync(resolved, candidate, throttle, cancellationToken));
            await Task.WhenAll(attempts).ConfigureAwait(false);
        }

        private async Task ProcessOneAsync(ResolvedSubscription resolved, InboxMessage candidate, SemaphoreSlim throttle, CancellationToken cancellationToken)
        {
            await throttle.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await ClaimAndReprocessAsync(resolved, candidate, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                throttle.Release();
            }
        }

        private async Task ClaimAndReprocessAsync(ResolvedSubscription resolved, InboxMessage candidate, CancellationToken cancellationToken)
        {
            var (subscription, store) = resolved;
            var leaseOwner = Guid.NewGuid().ToString("N");

            var claim = await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = candidate.MessageId,
                ChannelKey = candidate.ChannelKey,
                FeederId = candidate.FeederId,
                PartitionKey = candidate.PartitionKey,
                SchemaVersion = candidate.SchemaVersion,
                PayloadContentType = candidate.PayloadContentType,
                Payload = candidate.Payload,
                Headers = candidate.Headers,
                LeaseOwner = leaseOwner,
                LeaseDuration = subscription.Options.ClaimLeaseDuration,
            }, cancellationToken).ConfigureAwait(false);

            // Anything other than Claimed means another worker (or this same one, on a later poll)
            // already owns or resolved it since QueryRetryableAsync listed it - nothing to do.
            if (claim.Outcome != InboxClaimOutcome.Claimed)
                return;

            var claimed = claim.Message!;

            try
            {
                var handler = subscription.CreateHandler(_serviceProvider);
                await handler.HandleAsync(claimed, cancellationToken).ConfigureAwait(false);
                await store.CompleteAsync(claimed.Id, leaseOwner, cancellationToken).ConfigureAwait(false);
            }
            catch (InboxNonRetryableException exception)
            {
                await DeadLetterAsync(resolved, claimed, leaseOwner, exception.Message, exception, attemptsExhausted: false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                var reason = SanitizeFailureReason(exception);

                if (claimed.AttemptCount >= subscription.Options.MaxRetryAttempts)
                {
                    await DeadLetterAsync(resolved, claimed, leaseOwner, reason, exception, attemptsExhausted: true, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var nextRetryAtUtc = _timeProvider.GetUtcNow() + InboxRetryBackoff.Compute(subscription.Options, claimed.AttemptCount, _random);
                    await store.FailAsync(claimed.Id, leaseOwner, reason, nextRetryAtUtc, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task DeadLetterAsync(ResolvedSubscription resolved, InboxMessage claimed, string leaseOwner, string reason, Exception exception, bool attemptsExhausted, CancellationToken cancellationToken)
        {
            var deadLettered = await resolved.Store.DeadLetterAsync(claimed.Id, leaseOwner, reason, cancellationToken).ConfigureAwait(false);
            if (deadLettered is null)
                return;

            var handlers = resolved.Subscription.CreateDeadLetterHandlers;
            if (handlers.Count == 0)
                return;

            var category = InboxFailureClassifier.Classify(exception, attemptsExhausted);
            var context = InboxDeadLetterContextFactory.Create(deadLettered, category, resolved.Subscription.DeadLetterPayloadPolicy);
            var pipeline = new InboxDeadLetterPipeline([.. handlers.Select(create => create(_serviceProvider))]);

            // A failing handler must not undo or retry the dead-lettering that already durably succeeded
            // above - InboxDeadLetterPipeline itself isolates one handler's failure from every other, so
            // the only thing left to do with its result here is let it complete; nothing to recover.
            await pipeline.RunAsync(context, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Never persists <see cref="Exception.Message"/> as-is (it may embed payload content or other
        /// sensitive detail) - only the exception's type name, mirroring <c>InboxReceiveCoordinator</c>'s
        /// same rule for the live receive path.
        /// </summary>
        private static string SanitizeFailureReason(Exception exception)
        {
            var reason = exception.GetType().FullName ?? exception.GetType().Name;
            return reason.Length > InboxMessageLimits.MaxFailureReasonLength
                ? reason[..InboxMessageLimits.MaxFailureReasonLength]
                : reason;
        }

        public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

        private sealed record ResolvedSubscription(InboxRetrySubscription Subscription, IInboxStore Store);
    }
}
