using NSubstitute;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxPurgeWorkerTests
    {
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();
        private const string StoreName = "purge-store";

        [Fact]
        public async Task RunOnceAsync_ShouldPurgeProcessedEntriesOlderThanTheRetentionPeriod()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await CompleteOne(store, "old");

            timeProvider.Advance(TimeSpan.FromDays(8));
            var worker = BuildWorker(store, timeProvider, PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7) });

            await worker.RunOnceAsync();

            Assert.Null(await store.GetAsync("old", ChannelKey, null));
        }

        [Fact]
        public async Task RunOnceAsync_ShouldNeverPurgeAnEntryStillWithinItsRetentionPeriod()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await CompleteOne(store, "recent");

            timeProvider.Advance(TimeSpan.FromHours(1));
            var worker = BuildWorker(store, timeProvider, PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7) });

            await worker.RunOnceAsync();

            Assert.NotNull(await store.GetAsync("recent", ChannelKey, null));
        }

        [Fact]
        public async Task RunOnceAsync_ShouldApplyTheSeparateDeadLetterRetentionPeriod()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await CompleteOne(store, "processed");
            await DeadLetterOne(store, "dead-lettered");

            // Past Processed's 1-day retention but well within DeadLettered's 30-day retention.
            timeProvider.Advance(TimeSpan.FromDays(2));
            var options = PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(1), DeadLetterRetentionPeriod = TimeSpan.FromDays(30) };
            var worker = BuildWorker(store, timeProvider, options);

            await worker.RunOnceAsync();

            Assert.Null(await store.GetAsync("processed", ChannelKey, null));
            Assert.NotNull(await store.GetAsync("dead-lettered", ChannelKey, null));
        }

        [Fact]
        public async Task RunOnceAsync_ShouldRunMultipleBatchesUpToMaxPurgeBatchesPerRun()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            for (var i = 0; i < 5; i++)
                await CompleteOne(store, $"m{i}");

            timeProvider.Advance(TimeSpan.FromDays(8));
            var options = PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7), PurgeBatchSize = 1, MaxPurgeBatchesPerRun = 3 };
            var worker = BuildWorker(store, timeProvider, options, delayBetweenBatches: TimeSpan.Zero);

            await worker.RunOnceAsync();

            var remaining = 0;
            for (var i = 0; i < 5; i++)
                if (await store.GetAsync($"m{i}", ChannelKey, null) is not null)
                    remaining++;

            Assert.Equal(2, remaining); // 5 eligible, but bounded to 3 batches of 1 each
        }

        [Fact]
        public async Task RunOnceAsync_ShouldConsultTheHoldSourceAndNeverPurgeAHeldEntry()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var held = await CompleteOne(store, "held");
            await CompleteOne(store, "unheld");

            timeProvider.Advance(TimeSpan.FromDays(8));
            var options = PurgeOptions() with { RetentionPeriod = TimeSpan.FromDays(7) };
            var holdSource = new FixedHoldSource(new HashSet<Guid> { held });
            var worker = BuildWorker(store, timeProvider, options, holdSource: holdSource);

            await worker.RunOnceAsync();

            Assert.NotNull(await store.GetAsync("held", ChannelKey, null));
            Assert.Null(await store.GetAsync("unheld", ChannelKey, null));
        }

        [Fact]
        public async Task RunOnceAsync_CancelledBetweenBatches_ShouldStopIssuingFurtherBatches()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            for (var i = 0; i < 5; i++)
                await CompleteOne(store, $"m{i}");

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
                if (await store.GetAsync($"m{i}", ChannelKey, null) is not null)
                    remaining++;

            // Cancellation is checked before each batch, so exactly 2 batches (1 entry each) ran before
            // the loop observed cancellation and stopped - proving it never ran all 5 to completion.
            Assert.Equal(3, remaining);
        }

        private static InboxPurgeWorker BuildWorker(
            ReferenceInboxStore store,
            TimeProvider timeProvider,
            InboxOptions options,
            IInboxRetentionHoldSource? holdSource = null,
            TimeSpan? delayBetweenBatches = null)
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore(StoreName, InboxStoreType.InMemory).Returns(store);
            var subscription = new InboxPurgeSubscription
            {
                ChannelKey = ChannelKey,
                Options = options,
                CreateHoldSource = holdSource is null ? null : _ => holdSource,
            };

            return new InboxPurgeWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, delayBetweenBatches ?? TimeSpan.Zero);
        }

        private static async Task<Guid> CompleteOne(ReferenceInboxStore store, string messageId)
        {
            var claim = await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = messageId,
                ChannelKey = ChannelKey,
                FeederId = FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
                LeaseOwner = "owner-a",
                LeaseDuration = TimeSpan.FromMinutes(5),
            });
            await store.CompleteAsync(claim.Message!.Id, "owner-a");
            return claim.Message.Id;
        }

        private static async Task<Guid> DeadLetterOne(ReferenceInboxStore store, string messageId)
        {
            var claim = await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = messageId,
                ChannelKey = ChannelKey,
                FeederId = FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
                LeaseOwner = "owner-a",
                LeaseDuration = TimeSpan.FromMinutes(5),
            });
            await store.DeadLetterAsync(claim.Message!.Id, "owner-a", "boom");
            return claim.Message.Id;
        }

        private static InboxOptions PurgeOptions() => new()
        {
            InboxEnabled = true,
            StoreType = InboxStoreType.InMemory,
            StoreConnectionName = StoreName,
            PurgeBatchSize = 500,
            MaxPurgeBatchesPerRun = 20,
        };

        private sealed class FixedHoldSource(IReadOnlySet<Guid> heldIds) : IInboxRetentionHoldSource
        {
            public Task<IReadOnlySet<Guid>> GetHeldEntryIdsAsync(Guid channelKey, CancellationToken cancellationToken) =>
                Task.FromResult(heldIds);
        }

        private sealed class CancelingHoldSource(CancellationTokenSource cts, int cancelAfterCall) : IInboxRetentionHoldSource
        {
            private int _calls;

            public Task<IReadOnlySet<Guid>> GetHeldEntryIdsAsync(Guid channelKey, CancellationToken cancellationToken)
            {
                if (++_calls >= cancelAfterCall)
                    cts.Cancel();

                return Task.FromResult<IReadOnlySet<Guid>>(new HashSet<Guid>());
            }
        }
    }
}
