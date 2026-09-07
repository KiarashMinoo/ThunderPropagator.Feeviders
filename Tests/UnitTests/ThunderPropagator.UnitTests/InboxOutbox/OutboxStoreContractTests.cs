using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Backend-independent behavioral contract every <see cref="IOutboxStore"/> implementation must
    /// satisfy (issue #116's acceptance criteria). Subclass this against a real backend and implement
    /// <see cref="CreateStore"/> - passing this suite is the acceptance bar for "this store correctly
    /// implements <see cref="IOutboxStore"/>'s claim/ordering/recovery semantics", not just "it doesn't
    /// throw". <see cref="ReferenceOutboxStoreContractTests"/> runs it against an in-memory reference
    /// implementation to prove the suite itself catches the violations it claims to.
    /// </summary>
    public abstract class OutboxStoreContractTests
    {
        /// <summary>
        /// Creates a fresh, empty store for one test. <paramref name="timeProvider"/> must be the clock
        /// the store stamps <see cref="OutboxMessage"/> timestamps and lease expiry with, so tests can
        /// deterministically simulate lease expiry and retry eligibility.
        /// </summary>
        protected abstract IOutboxStore CreateStore(TimeProvider timeProvider);

        private static OutboxEnqueueRequest CreateRequest(string messageId, string? partitionKey = null) =>
            new()
            {
                MessageId = messageId,
                ProviderKey = "provider-a",
                PartitionKey = partitionKey,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
            };

        [Fact]
        public async Task ClaimBatchAsync_ConcurrentClaims_ShouldNeverClaimTheSameEntryTwice()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            for (var i = 0; i < 8; i++)
                await store.EnqueueAsync(CreateRequest($"m{i}"));

            var batches = await Task.WhenAll(Enumerable.Range(0, 8)
                .Select(i => store.ClaimBatchAsync(partitionKey: null, maxCount: 3, $"worker-{i}", TimeSpan.FromMinutes(5))));

            var claimedIds = batches.SelectMany(b => b.Select(m => m.Id)).ToArray();
            Assert.Equal(8, claimedIds.Length);
            Assert.Equal(8, claimedIds.Distinct().Count());
        }

        [Fact]
        public async Task ClaimBatchAsync_ShouldClaimInOrderingSequenceOrder()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            for (var i = 0; i < 5; i++)
                await store.EnqueueAsync(CreateRequest($"m{i}", partitionKey: "partition-a"));

            var claimed = await store.ClaimBatchAsync("partition-a", maxCount: 10, "worker-1", TimeSpan.FromMinutes(5));

            Assert.Equal(5, claimed.Count);
            Assert.Equal(claimed.Select(m => m.OrderingSequence).OrderBy(s => s), claimed.Select(m => m.OrderingSequence));
        }

        [Fact]
        public async Task ClaimBatchAsync_DifferentPartitions_ShouldClaimIndependently()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("a1", partitionKey: "partition-a"));
            await store.EnqueueAsync(CreateRequest("b1", partitionKey: "partition-b"));

            var claimedA = await store.ClaimBatchAsync("partition-a", maxCount: 10, "worker-a", TimeSpan.FromMinutes(5));

            Assert.Single(claimedA);
            Assert.Equal("a1", claimedA[0].MessageId);

            // Partition B is untouched by claiming partition A - still claimable by its own worker.
            var claimedB = await store.ClaimBatchAsync("partition-b", maxCount: 10, "worker-b", TimeSpan.FromMinutes(5));
            Assert.Single(claimedB);
            Assert.Equal("b1", claimedB[0].MessageId);
        }

        [Fact]
        public async Task ClaimBatchAsync_NullPartition_ShouldOnlyClaimEntriesWithNoPartitionKey()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("unpartitioned", partitionKey: null));
            await store.EnqueueAsync(CreateRequest("partitioned", partitionKey: "partition-a"));

            var claimed = await store.ClaimBatchAsync(partitionKey: null, maxCount: 10, "worker-1", TimeSpan.FromMinutes(5));

            Assert.Single(claimed);
            Assert.Equal("unpartitioned", claimed[0].MessageId);
        }

        [Fact]
        public async Task GetClaimablePartitionKeysAsync_ShouldReturnDistinctPartitionsWithClaimableEntries()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("a1", partitionKey: "partition-a"));
            await store.EnqueueAsync(CreateRequest("a2", partitionKey: "partition-a"));
            await store.EnqueueAsync(CreateRequest("b1", partitionKey: "partition-b"));
            await store.EnqueueAsync(CreateRequest("unpartitioned", partitionKey: null));

            var partitionKeys = await store.GetClaimablePartitionKeysAsync();

            Assert.Equal(3, partitionKeys.Count);
            Assert.Contains("partition-a", partitionKeys);
            Assert.Contains("partition-b", partitionKeys);
            Assert.Contains(null, partitionKeys);
        }

        [Fact]
        public async Task GetClaimablePartitionKeysAsync_ShouldExcludeAPartitionWithOnlyALiveLeaseOrTerminalEntries()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("leased", partitionKey: "partition-a"));
            await store.ClaimBatchAsync("partition-a", 10, "owner-a", TimeSpan.FromMinutes(5));

            await store.EnqueueAsync(CreateRequest("published", partitionKey: "partition-b"));
            var publishedClaim = await store.ClaimBatchAsync("partition-b", 10, "owner-a", TimeSpan.FromMinutes(5));
            await store.MarkPublishedAsync(publishedClaim[0].Id, "owner-a");

            Assert.Empty(await store.GetClaimablePartitionKeysAsync());
        }

        [Fact]
        public async Task ClaimBatchAsync_ShouldRecoverAnAbandonedLease()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("m1"));

            var firstClaim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(1));
            Assert.Single(firstClaim);

            var tooEarly = await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(1));
            Assert.Empty(tooEarly);

            timeProvider.Advance(TimeSpan.FromMinutes(2));

            var recovered = await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(1));
            Assert.Single(recovered);
            Assert.Equal("owner-b", recovered[0].LeaseOwner);
            // A crash-recovered claim is itself a new attempt - the same message may now be published
            // twice (once by whichever owner eventually completes it) if the abandoned owner was not
            // actually dead, which is the documented at-least-once duplicate this contract allows.
            Assert.Equal(2, recovered[0].Attempts);

            Assert.Null(await store.MarkPublishedAsync(firstClaim[0].Id, "owner-a"));
        }

        [Fact]
        public async Task ClaimBatchAsync_ShouldReclaimAFailedEntryOnceNextRetryAtUtcElapses()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));
            await store.MarkFailedAsync(claim[0].Id, "owner-a", "boom", timeProvider.GetUtcNow() + TimeSpan.FromMinutes(1));

            var tooEarly = await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(5));
            Assert.Empty(tooEarly);

            timeProvider.Advance(TimeSpan.FromMinutes(2));

            var retried = await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(5));
            Assert.Single(retried);
            Assert.Equal(OutboxMessageStatus.Publishing, retried[0].Status);
        }

        [Fact]
        public async Task ClaimBatchAsync_ShouldNotReclaimDeadLetteredOrPublishedEntries()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));

            await store.EnqueueAsync(CreateRequest("published"));
            var publishedClaim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));
            await store.MarkPublishedAsync(publishedClaim[0].Id, "owner-a");

            await store.EnqueueAsync(CreateRequest("dead"));
            var deadClaim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));
            await store.MarkDeadLetterAsync(deadClaim.Single(m => m.MessageId == "dead").Id, "owner-a", "unrecoverable");

            var remaining = await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(5));
            Assert.Empty(remaining);
        }

        [Fact]
        public async Task RenewLeaseAsync_ShouldPreventPrematureRecovery()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(1));

            timeProvider.Advance(TimeSpan.FromSeconds(50));
            Assert.NotNull(await store.RenewLeaseAsync(claim[0].Id, "owner-a", TimeSpan.FromMinutes(1)));

            timeProvider.Advance(TimeSpan.FromSeconds(50));
            var stillHeld = await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(1));
            Assert.Empty(stillHeld);
        }

        [Fact]
        public async Task RenewLeaseAsync_ShouldOnlySucceedForTheCurrentLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));

            Assert.Null(await store.RenewLeaseAsync(claim[0].Id, "owner-b", TimeSpan.FromMinutes(5)));
        }

        [Fact]
        public async Task MarkPublishedAsync_ShouldOnlySucceedForTheCurrentLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));

            Assert.Null(await store.MarkPublishedAsync(claim[0].Id, "owner-b"));

            var published = await store.MarkPublishedAsync(claim[0].Id, "owner-a");
            Assert.NotNull(published);
            Assert.Equal(OutboxMessageStatus.Published, published!.Status);
        }

        [Fact]
        public async Task MarkFailedAsync_ShouldOnlySucceedForTheCurrentLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));

            Assert.Null(await store.MarkFailedAsync(claim[0].Id, "owner-b", "boom", null));

            var failed = await store.MarkFailedAsync(claim[0].Id, "owner-a", "boom", null);
            Assert.NotNull(failed);
            Assert.Equal(OutboxMessageStatus.Failed, failed!.Status);
        }

        [Fact]
        public async Task MarkDeadLetterAsync_ShouldOnlySucceedForTheCurrentLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));

            Assert.Null(await store.MarkDeadLetterAsync(claim[0].Id, "owner-b", "unrecoverable"));

            var deadLettered = await store.MarkDeadLetterAsync(claim[0].Id, "owner-a", "unrecoverable");
            Assert.NotNull(deadLettered);
            Assert.Equal(OutboxMessageStatus.DeadLettered, deadLettered!.Status);
        }

        [Fact]
        public async Task ReleaseAsync_ShouldReturnToPendingPreservingOrderingSequenceWithoutRecordingFailure()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));
            var originalOrderingSequence = claim[0].OrderingSequence;

            Assert.True(await store.ReleaseAsync(claim[0].Id, "owner-a"));

            var reclaimed = await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(5));
            Assert.Single(reclaimed);
            Assert.Equal(originalOrderingSequence, reclaimed[0].OrderingSequence);
            Assert.Null(reclaimed[0].FailureReason);
        }

        [Fact]
        public async Task ReleaseAsync_ShouldRejectAMismatchedLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));

            Assert.False(await store.ReleaseAsync(claim[0].Id, "owner-b"));
        }

        [Fact]
        public async Task GetDepthAsync_NullPartition_ShouldAggregateAcrossEveryPartition()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("a1", partitionKey: "partition-a"));
            await store.EnqueueAsync(CreateRequest("b1", partitionKey: "partition-b"));
            await store.EnqueueAsync(CreateRequest("b2", partitionKey: "partition-b"));

            Assert.Equal(3, await store.GetDepthAsync(null));
            Assert.Equal(1, await store.GetDepthAsync("partition-a"));
            Assert.Equal(2, await store.GetDepthAsync("partition-b"));
        }

        [Fact]
        public async Task GetDepthAsync_ShouldExcludeTerminalEntries()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            await store.EnqueueAsync(CreateRequest("m1"));
            var claim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));
            await store.MarkPublishedAsync(claim[0].Id, "owner-a");

            Assert.Equal(0, await store.GetDepthAsync(null));
        }

        [Fact]
        public async Task GetOldestPendingAgeAsync_ShouldReflectTheOldestBacklogEntry()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("m1"));

            timeProvider.Advance(TimeSpan.FromMinutes(10));

            var age = await store.GetOldestPendingAgeAsync(null, timeProvider);
            Assert.Equal(TimeSpan.FromMinutes(10), age);
        }

        [Fact]
        public async Task GetOldestPendingAgeAsync_ShouldReturnNullWhenNothingIsPending()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));

            Assert.Null(await store.GetOldestPendingAgeAsync(null, new ManualTimeProvider(DateTimeOffset.UnixEpoch)));
        }

        [Fact]
        public async Task PurgeAsync_ShouldOnlyRemoveTerminalEntriesOlderThanTheCutoff()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);

            await store.EnqueueAsync(CreateRequest("old-published"));
            var publishedClaim = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));
            await store.MarkPublishedAsync(publishedClaim[0].Id, "owner-a");

            timeProvider.Advance(TimeSpan.FromDays(1));
            await store.EnqueueAsync(CreateRequest("active"));
            await store.ClaimBatchAsync(null, 10, "owner-b", TimeSpan.FromMinutes(5));

            var purged = await store.PurgeAsync(timeProvider.GetUtcNow());

            Assert.Equal(1, purged);
            Assert.Equal(1, await store.GetDepthAsync(null)); // the still-active entry is untouched
        }
    }
}
