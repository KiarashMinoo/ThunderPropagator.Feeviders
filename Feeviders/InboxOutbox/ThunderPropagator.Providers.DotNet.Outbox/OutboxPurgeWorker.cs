using System.Diagnostics;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Polls every registered <see cref="OutboxPurgeSubscription"/> and purges terminal
    /// (Published/DeadLettered) entries older than that Provider's configured retention windows via
    /// <see cref="IOutboxStore.PurgeAsync"/> - Published and DeadLettered entries age out on independent
    /// windows (<see cref="OutboxOptions.RetentionPeriod"/> and
    /// <see cref="OutboxOptions.DeadLetterRetentionPeriod"/> respectively), and a Provider's
    /// <see cref="IOutboxRetentionHoldSource"/> (if any) is consulted before every batch so a held entry
    /// is never purged regardless of age.
    /// </summary>
    /// <remarks>
    /// Hosting-agnostic by design (plain <see cref="StartAsync"/>/<see cref="StopAsync"/>, not
    /// <c>IHostedService</c>) - a caller's own host wires this into whatever hosting model it uses.
    /// <see cref="RunOnceAsync"/> runs one purge pass per subscription directly, without waiting on a
    /// polling interval - the entry point tests use to drive purging deterministically against a fake
    /// clock.
    /// </remarks>
    public sealed class OutboxPurgeWorker : IAsyncDisposable
    {
        private readonly IReadOnlyList<ResolvedSubscription> _subscriptions;
        private readonly IServiceProvider _serviceProvider;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _delayBetweenBatches;
        private CancellationTokenSource? _stoppingCts;
        private Task[]? _pollingLoops;

        /// <param name="subscriptions">One entry per Provider to purge. A subscription with <see cref="OutboxOptions.OutboxEnabled"/> false is skipped entirely - it does not even resolve a store.</param>
        /// <param name="storeFactory">Resolves each enabled subscription's store once, here at construction - not once per purge pass.</param>
        /// <param name="serviceProvider">Passed to <see cref="OutboxPurgeSubscription.CreateHoldSource"/> on every use, since a hold source may itself be scoped.</param>
        /// <param name="timeProvider">Clock used to compute retention cutoffs and drive the polling timer. Defaults to <see cref="TimeProvider.System"/> - tests should supply a fake for deterministic boundary-time assertions.</param>
        /// <param name="delayBetweenBatches">Pause between consecutive purge batches within one pass, so a large backlog never monopolizes the store in a tight loop. Defaults to 100ms; tests should pass <see cref="TimeSpan.Zero"/>.</param>
        /// <exception cref="ArgumentException">Two subscriptions share the same <see cref="OutboxPurgeSubscription.ProviderKey"/>, or an enabled subscription's options fail <see cref="OutboxOptions.Validate"/>.</exception>
        /// <exception cref="InvalidOperationException">An enabled subscription has no <see cref="OutboxOptions.StoreConnectionName"/>.</exception>
        public OutboxPurgeWorker(
            IEnumerable<OutboxPurgeSubscription> subscriptions,
            IOutboxStoreFactory storeFactory,
            IServiceProvider serviceProvider,
            TimeProvider? timeProvider = null,
            TimeSpan? delayBetweenBatches = null)
        {
            ArgumentNullException.ThrowIfNull(subscriptions);
            ArgumentNullException.ThrowIfNull(storeFactory);
            ArgumentNullException.ThrowIfNull(serviceProvider);

            _serviceProvider = serviceProvider;
            _timeProvider = timeProvider ?? TimeProvider.System;
            _delayBetweenBatches = delayBetweenBatches ?? TimeSpan.FromMilliseconds(100);
            _subscriptions = ResolveSubscriptions(subscriptions, storeFactory);
        }

        private static IReadOnlyList<ResolvedSubscription> ResolveSubscriptions(IEnumerable<OutboxPurgeSubscription> subscriptions, IOutboxStoreFactory storeFactory)
        {
            var seenProviders = new HashSet<string>();
            var resolved = new List<ResolvedSubscription>();

            foreach (var subscription in subscriptions)
            {
                if (!seenProviders.Add(subscription.ProviderKey))
                    throw new ArgumentException($"Provider '{subscription.ProviderKey}' has more than one Outbox purge subscription.", nameof(subscriptions));

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

        /// <summary>Runs one purge pass (up to <see cref="OutboxOptions.MaxPurgeBatchesPerRun"/> batches) per subscription and returns - does not wait on a polling interval.</summary>
        public async Task RunOnceAsync(CancellationToken cancellationToken = default)
        {
            foreach (var resolved in _subscriptions)
                await PurgeProviderAsync(resolved, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures every subscription's store is ready (see <see cref="IOutboxStoreInitializer"/>), then
        /// starts one polling loop per enabled subscription, each on its own
        /// <see cref="OutboxOptions.PurgePollingInterval"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException">Already started.</exception>
        /// <exception cref="OutboxStoreInitializationFailedException">A subscription's store implements <see cref="IOutboxStoreInitializer"/> and did not report <see cref="StoreInitializationResult.IsReady"/>.</exception>
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_stoppingCts is not null)
                throw new InvalidOperationException($"{nameof(OutboxPurgeWorker)} is already started.");

            foreach (var resolved in _subscriptions)
            {
                if (resolved.Store is not IOutboxStoreInitializer initializer)
                    continue;

                var result = await initializer.InitializeAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                if (!result.IsReady)
                    throw new OutboxStoreInitializationFailedException(resolved.Subscription.ProviderKey, result);
            }

            _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _pollingLoops = [.. _subscriptions.Select(resolved => PollAsync(resolved, _stoppingCts.Token))];
        }

        private async Task PollAsync(ResolvedSubscription resolved, CancellationToken cancellationToken)
        {
            using var timer = new PeriodicTimer(resolved.Subscription.Options.PurgePollingInterval, _timeProvider);

            try
            {
                while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                    await PurgeProviderAsync(resolved, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected on graceful stop.
            }
        }

        /// <summary>Cancels every polling loop (no new purge pass starts) and waits, up to <paramref name="drainTimeout"/>, for whatever pass each loop is currently mid-batch on to finish naturally.</summary>
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
                    // Best-effort drain.
                }
            }

            stoppingCts.Dispose();
        }

        private async Task PurgeProviderAsync(ResolvedSubscription resolved, CancellationToken cancellationToken)
        {
            var (subscription, store) = resolved;
            var options = subscription.Options;
            var now = _timeProvider.GetUtcNow();
            var publishedCutoff = now - options.RetentionPeriod;
            var deadLetteredCutoff = now - (options.DeadLetterRetentionPeriod ?? options.RetentionPeriod);
            var holdSource = subscription.CreateHoldSource?.Invoke(_serviceProvider);

            for (var batch = 0; batch < options.MaxPurgeBatchesPerRun; batch++)
            {
                if (cancellationToken.IsCancellationRequested)
                    return;

                var excludedIds = holdSource is null
                    ? null
                    : await holdSource.GetHeldEntryIdsAsync(cancellationToken).ConfigureAwait(false);

                var stopwatch = Stopwatch.StartNew();
                var result = await store.PurgeAsync(new OutboxPurgeRequest
                {
                    PublishedOlderThanUtc = publishedCutoff,
                    DeadLetteredOlderThanUtc = deadLetteredCutoff,
                    MaxCount = options.PurgeBatchSize,
                    ExcludedIds = excludedIds,
                }, cancellationToken).ConfigureAwait(false);

                var providerTag = new KeyValuePair<string, object?>("provider", subscription.ProviderKey);
                OutboxPurgeTelemetry.BatchesRun.Add(1, providerTag);
                OutboxPurgeTelemetry.EntriesPurged.Add(result.PurgedCount, providerTag);
                OutboxPurgeTelemetry.BatchDuration.Record(stopwatch.Elapsed.TotalMilliseconds, providerTag);

                if (!result.HasMore)
                    return;

                if (batch < options.MaxPurgeBatchesPerRun - 1)
                    await Task.Delay(_delayBetweenBatches, cancellationToken).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

        private sealed record ResolvedSubscription(OutboxPurgeSubscription Subscription, IOutboxStore Store);
    }
}
