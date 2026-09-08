using System.Diagnostics;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Polls every registered <see cref="InboxPurgeSubscription"/> and purges terminal
    /// (Processed/DeadLettered) entries older than that channel's configured retention windows via
    /// <see cref="IInboxStore.PurgeAsync"/> - Processed and DeadLettered entries age out on independent
    /// windows (<see cref="InboxOptions.RetentionPeriod"/> and
    /// <see cref="InboxOptions.DeadLetterRetentionPeriod"/> respectively), and a channel's
    /// <see cref="IInboxRetentionHoldSource"/> (if any) is consulted before every batch so a held entry
    /// is never purged regardless of age.
    /// </summary>
    /// <remarks>
    /// Hosting-agnostic by design (plain <see cref="StartAsync"/>/<see cref="StopAsync"/>, not
    /// <c>IHostedService</c>) - a caller's own host wires this into whatever hosting model it uses, the
    /// same pattern <see cref="InboxRetryWorker"/> follows. <see cref="RunOnceAsync"/> runs one purge
    /// pass per subscription directly, without waiting on a polling interval - the entry point tests use
    /// to drive purging deterministically against a fake clock.
    /// </remarks>
    public sealed class InboxPurgeWorker : IAsyncDisposable
    {
        private readonly IReadOnlyList<ResolvedSubscription> _subscriptions;
        private readonly IServiceProvider _serviceProvider;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _delayBetweenBatches;
        private CancellationTokenSource? _stoppingCts;
        private Task[]? _pollingLoops;

        /// <param name="subscriptions">One entry per channel to purge. A subscription with <see cref="InboxOptions.InboxEnabled"/> false is skipped entirely - it does not even resolve a store.</param>
        /// <param name="storeFactory">Resolves each enabled subscription's store once, here at construction - not once per purge pass.</param>
        /// <param name="serviceProvider">Passed to <see cref="InboxPurgeSubscription.CreateHoldSource"/> on every use, since a hold source may itself be scoped.</param>
        /// <param name="timeProvider">Clock used to compute retention cutoffs and drive the polling timer. Defaults to <see cref="TimeProvider.System"/> - tests should supply a fake for deterministic boundary-time assertions.</param>
        /// <param name="delayBetweenBatches">Pause between consecutive purge batches within one pass, so a large backlog never monopolizes the store in a tight loop. Defaults to 100ms; tests should pass <see cref="TimeSpan.Zero"/>.</param>
        /// <exception cref="ArgumentException">Two subscriptions share the same <see cref="InboxPurgeSubscription.ChannelKey"/>, or an enabled subscription's options fail <see cref="InboxOptions.Validate"/>.</exception>
        /// <exception cref="InvalidOperationException">An enabled subscription has no <see cref="InboxOptions.StoreConnectionName"/>.</exception>
        public InboxPurgeWorker(
            IEnumerable<InboxPurgeSubscription> subscriptions,
            IInboxStoreFactory storeFactory,
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

        private static IReadOnlyList<ResolvedSubscription> ResolveSubscriptions(IEnumerable<InboxPurgeSubscription> subscriptions, IInboxStoreFactory storeFactory)
        {
            var seenChannels = new HashSet<Guid>();
            var resolved = new List<ResolvedSubscription>();

            foreach (var subscription in subscriptions)
            {
                if (!seenChannels.Add(subscription.ChannelKey))
                    throw new ArgumentException($"Channel '{subscription.ChannelKey}' has more than one Inbox purge subscription.", nameof(subscriptions));

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

        /// <summary>Runs one purge pass (up to <see cref="InboxOptions.MaxPurgeBatchesPerRun"/> batches) per subscription and returns - does not wait on a polling interval.</summary>
        public async Task RunOnceAsync(CancellationToken cancellationToken = default)
        {
            foreach (var resolved in _subscriptions)
                await PurgeChannelAsync(resolved, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures every subscription's store is ready (see <see cref="IInboxStoreInitializer"/>), then
        /// starts one polling loop per enabled subscription, each on its own
        /// <see cref="InboxOptions.PurgePollingInterval"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException">Already started.</exception>
        /// <exception cref="InboxStoreInitializationFailedException">A subscription's store implements <see cref="IInboxStoreInitializer"/> and did not report <see cref="StoreInitializationResult.IsReady"/>.</exception>
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_stoppingCts is not null)
                throw new InvalidOperationException($"{nameof(InboxPurgeWorker)} is already started.");

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
            using var timer = new PeriodicTimer(resolved.Subscription.Options.PurgePollingInterval, _timeProvider);

            try
            {
                while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                    await PurgeChannelAsync(resolved, cancellationToken).ConfigureAwait(false);
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

        private async Task PurgeChannelAsync(ResolvedSubscription resolved, CancellationToken cancellationToken)
        {
            var (subscription, store) = resolved;
            var options = subscription.Options;
            var now = _timeProvider.GetUtcNow();
            var processedCutoff = now - options.RetentionPeriod;
            var deadLetteredCutoff = now - (options.DeadLetterRetentionPeriod ?? options.RetentionPeriod);
            var holdSource = subscription.CreateHoldSource?.Invoke(_serviceProvider);

            for (var batch = 0; batch < options.MaxPurgeBatchesPerRun; batch++)
            {
                if (cancellationToken.IsCancellationRequested)
                    return;

                var excludedIds = holdSource is null
                    ? null
                    : await holdSource.GetHeldEntryIdsAsync(subscription.ChannelKey, cancellationToken).ConfigureAwait(false);

                var stopwatch = Stopwatch.StartNew();
                var result = await store.PurgeAsync(new InboxPurgeRequest
                {
                    ChannelKey = subscription.ChannelKey,
                    ProcessedOlderThanUtc = processedCutoff,
                    DeadLetteredOlderThanUtc = deadLetteredCutoff,
                    MaxCount = options.PurgeBatchSize,
                    ExcludedIds = excludedIds,
                }, cancellationToken).ConfigureAwait(false);

                var channelTag = new KeyValuePair<string, object?>("channel", subscription.ChannelKey);
                InboxPurgeTelemetry.BatchesRun.Add(1, channelTag);
                InboxPurgeTelemetry.EntriesPurged.Add(result.PurgedCount, channelTag);
                InboxPurgeTelemetry.BatchDuration.Record(stopwatch.Elapsed.TotalMilliseconds, channelTag);

                if (!result.HasMore)
                    return;

                if (batch < options.MaxPurgeBatchesPerRun - 1)
                    await Task.Delay(_delayBetweenBatches, cancellationToken).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

        private sealed record ResolvedSubscription(InboxPurgeSubscription Subscription, IInboxStore Store);
    }
}
