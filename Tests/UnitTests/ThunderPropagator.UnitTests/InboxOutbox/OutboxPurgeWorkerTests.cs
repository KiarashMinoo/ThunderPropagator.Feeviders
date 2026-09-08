using NSubstitute;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxPurgeWorkerTests
    {
        private const string ProviderKey = "provider-a";
        private const string StoreName = "purge-store";

        [Fact]
        public async Task RunOnceAsync_ShouldPurgePublishedEntriesOlderThanTheRetentionPeriod()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await PublishOne(store, "old");

            timeProvider.Advance(TimeSpan.FromDays(8));
            var worker = BuildWorker(store, timeProvider, PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7) });

            await worker.RunOnceAsync();

            Assert.Null(store.TryPeek("old"));
        }

        [Fact]
        public async Task RunOnceAsync_ShouldNeverPurgeAnEntryStillWithinItsRetentionPeriod()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await PublishOne(store, "recent");

            timeProvider.Advance(TimeSpan.FromHours(1));
            var worker = BuildWorker(store, timeProvider, PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7) });

            await worker.RunOnceAsync();

            Assert.NotNull(store.TryPeek("recent"));
        }

        [Fact]
        public async Task RunOnceAsync_ShouldApplyTheSeparateDeadLetterRetentionPeriod()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await PublishOne(store, "published");
            await DeadLetterOne(store, "dead-lettered");

            // Past Published's 1-day retention but well within DeadLettered's 30-day retention.
            timeProvider.Advance(TimeSpan.FromDays(2));
            var options = PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(1), DeadLetterRetentionPeriod = TimeSpan.FromDays(30) };
            var worker = BuildWorker(store, timeProvider, options);

            await worker.RunOnceAsync();

            Assert.Null(store.TryPeek("published"));
            Assert.NotNull(store.TryPeek("dead-lettered"));
        }

        [Fact]
        public async Task RunOnceAsync_ShouldRunMultipleBatchesUpToMaxPurgeBatchesPerRun()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            for (var i = 0; i < 5; i++)
                await PublishOne(store, $"m{i}");

            timeProvider.Advance(TimeSpan.FromDays(8));
            var options = PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7), PurgeBatchSize = 1, MaxPurgeBatchesPerRun = 3 };
            var worker = BuildWorker(store, timeProvider, options, delayBetweenBatches: TimeSpan.Zero);

            await worker.RunOnceAsync();

            var remaining = 0;
            for (var i = 0; i < 5; i++)
                if (store.TryPeek($"m{i}") is not null)
                    remaining++;

            Assert.Equal(2, remaining); // 5 eligible, but bounded to 3 batches of 1 each
        }

        [Fact]
        public async Task RunOnceAsync_ShouldConsultTheHoldSourceAndNeverPurgeAHeldEntry()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            var held = await PublishOne(store, "held");
            await PublishOne(store, "unheld");

            timeProvider.Advance(TimeSpan.FromDays(8));
            var options = PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7) };
            var holdSource = new FixedHoldSource(new HashSet<Guid> { held });
            var worker = BuildWorker(store, timeProvider, options, holdSource: holdSource);

            await worker.RunOnceAsync();

            Assert.NotNull(store.TryPeek("held"));
            Assert.Null(store.TryPeek("unheld"));
        }

        [Fact]
        public async Task RunOnceAsync_CancelledBetweenBatches_ShouldStopIssuingFurtherBatches()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            for (var i = 0; i < 5; i++)
                await PublishOne(store, $"m{i}");

            timeProvider.Advance(TimeSpan.FromDays(8));
            var options = PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7), PurgeBatchSize = 1, MaxPurgeBatchesPerRun = 10 };
            using var cts = new CancellationTokenSource();
            var holdSource = new CancelingHoldSource(cts, cancelAfterCall: 2);
            var worker = BuildWorker(store, timeProvider, options, holdSource: holdSource, delayBetweenBatches: TimeSpan.Zero);

            // The 2nd batch's own hold-source call cancels the token mid-batch - that batch's purge still
            // completes (already in flight), but the inter-batch delay immediately afterward observes the
            // now-cancelled token and throws, so a 3rd batch never starts.
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => worker.RunOnceAsync(cts.Token));

            var remaining = 0;
            for (var i = 0; i < 5; i++)
                if (store.TryPeek($"m{i}") is not null)
                    remaining++;

            Assert.Equal(3, remaining);
        }

        private static OutboxPurgeWorker BuildWorker(
            ReferenceOutboxStore store,
            TimeProvider timeProvider,
            OutboxOptions options,
            IOutboxRetentionHoldSource? holdSource = null,
            TimeSpan? delayBetweenBatches = null)
        {
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            storeFactory.GetStore(StoreName, OutboxStoreType.InMemory).Returns(store);
            var subscription = new OutboxPurgeSubscription
            {
                ProviderKey = ProviderKey,
                Options = options,
                CreateHoldSource = holdSource is null ? null : _ => holdSource,
            };

            return new OutboxPurgeWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, delayBetweenBatches ?? TimeSpan.Zero);
        }

        private static async Task<Guid> PublishOne(ReferenceOutboxStore store, string messageId)
        {
            await store.EnqueueAsync(new OutboxEnqueueRequest
            {
                MessageId = messageId,
                ProviderKey = ProviderKey,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
            });
            var claim = (await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5))).Single(m => m.MessageId == messageId);
            await store.MarkPublishedAsync(claim.Id, "owner-a");
            return claim.Id;
        }

        private static async Task<Guid> DeadLetterOne(ReferenceOutboxStore store, string messageId)
        {
            await store.EnqueueAsync(new OutboxEnqueueRequest
            {
                MessageId = messageId,
                ProviderKey = ProviderKey,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
            });
            var claim = (await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5))).Single(m => m.MessageId == messageId);
            await store.MarkDeadLetterAsync(claim.Id, "owner-a", "boom");
            return claim.Id;
        }

        private static OutboxOptions PurgeOptions() => new()
        {
            OutboxEnabled = true,
            StoreType = OutboxStoreType.InMemory,
            StoreConnectionName = StoreName,
            PurgeBatchSize = 500,
            MaxPurgeBatchesPerRun = 20,
        };

        private sealed class FixedHoldSource(IReadOnlySet<Guid> heldIds) : IOutboxRetentionHoldSource
        {
            public Task<IReadOnlySet<Guid>> GetHeldEntryIdsAsync(CancellationToken cancellationToken) =>
                Task.FromResult(heldIds);
        }

        private sealed class CancelingHoldSource(CancellationTokenSource cts, int cancelAfterCall) : IOutboxRetentionHoldSource
        {
            private int _calls;

            public Task<IReadOnlySet<Guid>> GetHeldEntryIdsAsync(CancellationToken cancellationToken)
            {
                if (++_calls >= cancelAfterCall)
                    cts.Cancel();

                return Task.FromResult<IReadOnlySet<Guid>>(new HashSet<Guid>());
            }
        }
    }
}
