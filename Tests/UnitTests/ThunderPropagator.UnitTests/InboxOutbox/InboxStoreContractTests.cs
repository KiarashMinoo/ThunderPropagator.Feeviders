using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Backend-independent behavioral contract every <see cref="IInboxStore"/> implementation must
    /// satisfy (issue #109's acceptance criteria). Subclass this against a real backend and implement
    /// <see cref="CreateStore"/> - passing this suite is the acceptance bar for "this store correctly
    /// implements <see cref="IInboxStore"/>'s claim/retry semantics", not just "it doesn't throw".
    /// <see cref="ReferenceInboxStoreContractTests"/> runs it against an in-memory reference
    /// implementation to prove the suite itself catches the violations it claims to.
    /// </summary>
    public abstract class InboxStoreContractTests
    {
        /// <summary>
        /// Creates a fresh, empty store for one test. <paramref name="timeProvider"/> must be the clock
        /// the store stamps <see cref="InboxMessage"/> timestamps and lease expiry with, so tests can
        /// deterministically simulate lease expiry and retry eligibility.
        /// </summary>
        protected abstract IInboxStore CreateStore(TimeProvider timeProvider);

        private static InboxClaimRequest CreateRequest(string messageId, Guid channelKey, string leaseOwner, TimeSpan leaseDuration, string? partitionKey = null) =>
            new()
            {
                MessageId = messageId,
                ChannelKey = channelKey,
                FeederId = Guid.NewGuid(),
                PartitionKey = partitionKey,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
                LeaseOwner = leaseOwner,
                LeaseDuration = leaseDuration,
            };

        [Fact]
        public async Task TryClaimAsync_ConcurrentDuplicateReceives_ShouldYieldExactlyOneClaim()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            var channelKey = Guid.NewGuid();

            var results = await Task.WhenAll(Enumerable.Range(0, 8)
                .Select(i => store.TryClaimAsync(CreateRequest("duplicate-message", channelKey, $"worker-{i}", TimeSpan.FromMinutes(5)))));

            Assert.Single(results, r => r.Outcome == InboxClaimOutcome.Claimed);
            Assert.All(results.Where(r => r.Outcome != InboxClaimOutcome.Claimed),
                r => Assert.Equal(InboxClaimOutcome.ClaimedByAnotherOwner, r.Outcome));
        }

        [Fact]
        public async Task CompleteAsync_ShouldOnlySucceedForTheCurrentLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            var claim = await store.TryClaimAsync(CreateRequest("m1", Guid.NewGuid(), "owner-a", TimeSpan.FromMinutes(5)));
            Assert.Equal(InboxClaimOutcome.Claimed, claim.Outcome);

            Assert.Null(await store.CompleteAsync(claim.Message!.Id, "owner-b"));

            var completed = await store.CompleteAsync(claim.Message.Id, "owner-a");
            Assert.NotNull(completed);
            Assert.Equal(InboxMessageStatus.Processed, completed!.Status);
        }

        [Fact]
        public async Task FailAsync_ShouldOnlySucceedForTheCurrentLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            var claim = await store.TryClaimAsync(CreateRequest("m1", Guid.NewGuid(), "owner-a", TimeSpan.FromMinutes(5)));

            Assert.Null(await store.FailAsync(claim.Message!.Id, "owner-b", "boom", null));

            var failed = await store.FailAsync(claim.Message.Id, "owner-a", "boom", null);
            Assert.NotNull(failed);
            Assert.Equal(InboxMessageStatus.Failed, failed!.Status);
        }

        [Fact]
        public async Task TryClaimAsync_ShouldRecoverAnExpiredLease()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            var channelKey = Guid.NewGuid();

            var firstClaim = await store.TryClaimAsync(CreateRequest("m1", channelKey, "owner-a", TimeSpan.FromMinutes(1)));
            Assert.Equal(InboxClaimOutcome.Claimed, firstClaim.Outcome);

            var tooEarly = await store.TryClaimAsync(CreateRequest("m1", channelKey, "owner-b", TimeSpan.FromMinutes(1)));
            Assert.Equal(InboxClaimOutcome.ClaimedByAnotherOwner, tooEarly.Outcome);

            timeProvider.Advance(TimeSpan.FromMinutes(2));

            var recovered = await store.TryClaimAsync(CreateRequest("m1", channelKey, "owner-b", TimeSpan.FromMinutes(1)));
            Assert.Equal(InboxClaimOutcome.Claimed, recovered.Outcome);
            Assert.Equal("owner-b", recovered.Message!.LeaseOwner);
            Assert.Equal(2, recovered.Message.AttemptCount);

            Assert.Null(await store.CompleteAsync(firstClaim.Message!.Id, "owner-a"));
        }

        [Fact]
        public async Task RenewLeaseAsync_ShouldPreventPrematureRecovery()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            var channelKey = Guid.NewGuid();

            var claim = await store.TryClaimAsync(CreateRequest("m1", channelKey, "owner-a", TimeSpan.FromMinutes(1)));
            timeProvider.Advance(TimeSpan.FromSeconds(50));
            Assert.NotNull(await store.RenewLeaseAsync(claim.Message!.Id, "owner-a", TimeSpan.FromMinutes(1)));

            timeProvider.Advance(TimeSpan.FromSeconds(50));
            var stillHeld = await store.TryClaimAsync(CreateRequest("m1", channelKey, "owner-b", TimeSpan.FromMinutes(1)));
            Assert.Equal(InboxClaimOutcome.ClaimedByAnotherOwner, stillHeld.Outcome);
        }

        [Fact]
        public async Task RenewLeaseAsync_ShouldOnlySucceedForTheCurrentLeaseOwner()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            var claim = await store.TryClaimAsync(CreateRequest("m1", Guid.NewGuid(), "owner-a", TimeSpan.FromMinutes(5)));

            Assert.Null(await store.RenewLeaseAsync(claim.Message!.Id, "owner-b", TimeSpan.FromMinutes(5)));
        }

        [Fact]
        public async Task TryClaimAsync_ShouldReportAlreadyProcessedAndDeadLetteredWithoutReclaiming()
        {
            var store = CreateStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            var channelKey = Guid.NewGuid();

            var processedClaim = await store.TryClaimAsync(CreateRequest("processed-message", channelKey, "owner-a", TimeSpan.FromMinutes(5)));
            await store.CompleteAsync(processedClaim.Message!.Id, "owner-a");
            var processedRetry = await store.TryClaimAsync(CreateRequest("processed-message", channelKey, "owner-b", TimeSpan.FromMinutes(5)));
            Assert.Equal(InboxClaimOutcome.AlreadyProcessed, processedRetry.Outcome);

            var deadLetterClaim = await store.TryClaimAsync(CreateRequest("dead-message", channelKey, "owner-a", TimeSpan.FromMinutes(5)));
            await store.DeadLetterAsync(deadLetterClaim.Message!.Id, "owner-a", "unrecoverable");
            var deadLetterRetry = await store.TryClaimAsync(CreateRequest("dead-message", channelKey, "owner-b", TimeSpan.FromMinutes(5)));
            Assert.Equal(InboxClaimOutcome.DeadLettered, deadLetterRetry.Outcome);
        }

        [Fact]
        public async Task QueryRetryableAsync_ShouldNotClaimTheEntriesItReturns()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            var channelKey = Guid.NewGuid();

            var claim = await store.TryClaimAsync(CreateRequest("retryable", channelKey, "owner-a", TimeSpan.FromMinutes(5)));
            await store.FailAsync(claim.Message!.Id, "owner-a", "boom", timeProvider.GetUtcNow() - TimeSpan.FromSeconds(1));

            var firstQuery = await store.QueryRetryableAsync(channelKey, maxCount: 10);
            var secondQuery = await store.QueryRetryableAsync(channelKey, maxCount: 10);

            Assert.Contains(firstQuery, m => m.Id == claim.Message.Id);
            Assert.Contains(secondQuery, m => m.Id == claim.Message.Id);
            Assert.Equal(InboxMessageStatus.Failed, (await store.GetAsync("retryable", channelKey, null))!.Status);
        }

        [Fact]
        public async Task QueryRetryableAsync_FollowedByConcurrentClaims_ShouldStillYieldExactlyOneClaim()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            var channelKey = Guid.NewGuid();

            var claim = await store.TryClaimAsync(CreateRequest("retry-race", channelKey, "owner-a", TimeSpan.FromMinutes(5)));
            await store.FailAsync(claim.Message!.Id, "owner-a", "boom", timeProvider.GetUtcNow() - TimeSpan.FromSeconds(1));

            var retryable = await store.QueryRetryableAsync(channelKey, maxCount: 10);
            Assert.Contains(retryable, m => m.Id == claim.Message.Id);

            var results = await Task.WhenAll(Enumerable.Range(0, 8)
                .Select(i => store.TryClaimAsync(CreateRequest("retry-race", channelKey, $"retry-worker-{i}", TimeSpan.FromMinutes(5)))));

            Assert.Single(results, r => r.Outcome == InboxClaimOutcome.Claimed);
        }

        [Fact]
        public async Task PurgeAsync_ShouldOnlyRemoveTerminalEntriesOlderThanTheCutoff()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = CreateStore(timeProvider);
            var channelKey = Guid.NewGuid();

            var processedClaim = await store.TryClaimAsync(CreateRequest("old-processed", channelKey, "owner-a", TimeSpan.FromMinutes(5)));
            await store.CompleteAsync(processedClaim.Message!.Id, "owner-a");

            timeProvider.Advance(TimeSpan.FromDays(1));
            await store.TryClaimAsync(CreateRequest("active", channelKey, "owner-b", TimeSpan.FromMinutes(5)));

            var purged = await store.PurgeAsync(channelKey, timeProvider.GetUtcNow());

            Assert.Equal(1, purged);
            Assert.Null(await store.GetAsync("old-processed", channelKey, null));
            Assert.Equal(InboxMessageStatus.Processing, (await store.GetAsync("active", channelKey, null))!.Status);
        }
    }
}
