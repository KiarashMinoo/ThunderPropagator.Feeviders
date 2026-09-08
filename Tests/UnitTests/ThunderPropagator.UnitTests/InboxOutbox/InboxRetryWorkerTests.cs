using NSubstitute;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxRetryWorkerTests
    {
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();
        private const string StoreName = "retry-store";

        [Fact]
        public async Task RunOnceAsync_HandlerSucceeds_ShouldCompleteTheEntry()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await FailOnce(store, timeProvider, "m1");

            var invoked = false;
            var worker = BuildWorker(store, timeProvider, DelegateHandler.Success(_ => invoked = true));

            await worker.RunOnceAsync();

            Assert.True(invoked);
            var message = await store.GetAsync("m1", ChannelKey, null);
            Assert.Equal(InboxMessageStatus.Processed, message!.Status);
        }

        [Fact]
        public async Task RunOnceAsync_HandlerThrows_BelowMaxAttempts_ShouldFailWithComputedBackoff()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await FailOnce(store, timeProvider, "m1"); // AttemptCount == 1 going into the retry below

            var options = RetryOptions() with { MaxRetryAttempts = 5 };
            var random = new FixedRandom(1.0); // full jitter fraction => exact exponential ceiling
            var worker = BuildWorker(store, timeProvider, DelegateHandler.Throwing(new InvalidOperationException("boom")), options, random);

            await worker.RunOnceAsync();

            var message = await store.GetAsync("m1", ChannelKey, null);
            Assert.Equal(InboxMessageStatus.Failed, message!.Status);
            Assert.Equal(2, message.AttemptCount);
            // AttemptCount is 2 by the time the retry's own failure recomputes backoff.
            var expectedDelay = InboxRetryBackoff.Compute(options, attemptCount: 2, new FixedRandom(1.0));
            Assert.Equal(timeProvider.GetUtcNow() + expectedDelay, message.NextRetryAtUtc);
        }

        [Fact]
        public async Task RunOnceAsync_HandlerThrows_AtMaxAttempts_ShouldDeadLetter()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await FailOnce(store, timeProvider, "m1"); // AttemptCount == 1

            var options = RetryOptions() with { MaxRetryAttempts = 1 };
            var worker = BuildWorker(store, timeProvider, DelegateHandler.Throwing(new InvalidOperationException("boom")), options);

            await worker.RunOnceAsync();

            var message = await store.GetAsync("m1", ChannelKey, null);
            Assert.Equal(InboxMessageStatus.DeadLettered, message!.Status);
        }

        [Fact]
        public async Task RunOnceAsync_NonRetryableException_ShouldDeadLetterImmediatelyRegardlessOfRemainingAttempts()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await FailOnce(store, timeProvider, "m1"); // AttemptCount == 1, well below MaxRetryAttempts

            var options = RetryOptions() with { MaxRetryAttempts = 50 };
            var worker = BuildWorker(store, timeProvider, DelegateHandler.Throwing(new InboxNonRetryableException("unrecoverable")), options);

            await worker.RunOnceAsync();

            var message = await store.GetAsync("m1", ChannelKey, null);
            Assert.Equal(InboxMessageStatus.DeadLettered, message!.Status);
        }

        [Fact]
        public async Task RunOnceAsync_FailureReason_ShouldNeverPersistTheExceptionMessage()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await FailOnce(store, timeProvider, "m1");

            var options = RetryOptions() with { MaxRetryAttempts = 5 };
            var worker = BuildWorker(store, timeProvider, DelegateHandler.Throwing(new InvalidOperationException("payload had secret order-42")), options);

            await worker.RunOnceAsync();

            var message = await store.GetAsync("m1", ChannelKey, null);
            Assert.DoesNotContain("secret", message!.FailureReason);
            Assert.DoesNotContain("order-42", message.FailureReason);
            Assert.Contains(nameof(InvalidOperationException), message.FailureReason);
        }

        [Fact]
        public async Task RunOnceAsync_AbandonedProcessingLease_ShouldBeReclaimedAndReprocessed()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var options = RetryOptions() with { ClaimLeaseDuration = TimeSpan.FromMinutes(1) };

            // Simulate a worker that claimed and then crashed - never Completed/Failed/DeadLettered.
            var abandoned = await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = "abandoned",
                ChannelKey = ChannelKey,
                FeederId = FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
                LeaseOwner = "crashed-worker",
                LeaseDuration = options.ClaimLeaseDuration,
            });
            Assert.Equal(InboxMessageStatus.Processing, abandoned.Message!.Status);

            timeProvider.Advance(TimeSpan.FromMinutes(2));

            var invoked = false;
            var worker = BuildWorker(store, timeProvider, DelegateHandler.Success(_ => invoked = true), options);

            await worker.RunOnceAsync();

            Assert.True(invoked);
            var message = await store.GetAsync("abandoned", ChannelKey, null);
            Assert.Equal(InboxMessageStatus.Processed, message!.Status);
        }

        [Fact]
        public async Task RunOnceAsync_TwoWorkersRacingTheSameEntry_ShouldInvokeTheHandlerExactlyOnce()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await FailOnce(store, timeProvider, "m1");

            var invocationCount = 0;
            var handler = DelegateHandler.Success(_ => Interlocked.Increment(ref invocationCount));
            var workerA = BuildWorker(store, timeProvider, handler);
            var workerB = BuildWorker(store, timeProvider, handler);

            await Task.WhenAll(workerA.RunOnceAsync(), workerB.RunOnceAsync());

            Assert.Equal(1, invocationCount);
            var message = await store.GetAsync("m1", ChannelKey, null);
            Assert.Equal(InboxMessageStatus.Processed, message!.Status);
        }

        [Fact]
        public async Task RunOnceAsync_DeadLetter_ShouldInvokeTheRegisteredDeadLetterHandler()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            await FailOnce(store, timeProvider, "m1");

            InboxMessage? notified = null;
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore(StoreName, InboxStoreType.InMemory).Returns(store);
            var options = RetryOptions() with { MaxRetryAttempts = 1 };
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = options,
                CreateHandler = _ => DelegateHandler.Throwing(new InboxNonRetryableException("unrecoverable")),
                CreateDeadLetterHandler = _ => new DelegateDeadLetterHandler((message, _, _) =>
                {
                    notified = message;
                    return ValueTask.CompletedTask;
                }),
            };
            var worker = new InboxRetryWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, new FixedRandom(0.5));

            await worker.RunOnceAsync();

            Assert.NotNull(notified);
            Assert.Equal("m1", notified!.MessageId);
        }

        [Fact]
        public void Constructor_DuplicateChannelKey_ShouldThrow()
        {
            var options = RetryOptions();
            var subscription = new InboxRetrySubscription { ChannelKey = ChannelKey, Options = options, CreateHandler = _ => DelegateHandler.Success(_ => { }) };

            Assert.Throws<ArgumentException>(() => new InboxRetryWorker(
                [subscription, subscription with { }], Substitute.For<IInboxStoreFactory>(), Substitute.For<IServiceProvider>()));
        }

        [Fact]
        public void Constructor_EnabledWithoutStoreConnectionName_ShouldThrow()
        {
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = RetryOptions() with { StoreConnectionName = null, StoreType = InboxStoreType.InMemory },
                CreateHandler = _ => DelegateHandler.Success(_ => { }),
            };

            Assert.Throws<InvalidOperationException>(() => new InboxRetryWorker(
                [subscription], Substitute.For<IInboxStoreFactory>(), Substitute.For<IServiceProvider>()));
        }

        [Fact]
        public void Constructor_DisabledSubscription_ShouldNeverResolveAStore()
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = new InboxOptions(), // InboxEnabled defaults to false
                CreateHandler = _ => DelegateHandler.Success(_ => { }),
            };

            _ = new InboxRetryWorker([subscription], storeFactory, Substitute.For<IServiceProvider>());

            storeFactory.DidNotReceiveWithAnyArgs().GetStore(default!, default);
        }

        [Fact]
        public async Task StartAsync_ThenStopAsync_ShouldPollAtLeastOnceAndShutDownCleanly()
        {
            var store = new ReferenceInboxStore(TimeProvider.System);
            await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = "m1",
                ChannelKey = ChannelKey,
                FeederId = FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1],
                LeaseOwner = "owner-a",
                LeaseDuration = TimeSpan.FromMilliseconds(1),
            });
            await Task.Delay(TimeSpan.FromMilliseconds(20));

            var tcs = new TaskCompletionSource();
            var options = RetryOptions() with { RetryPollingInterval = TimeSpan.FromMilliseconds(10) };
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore(StoreName, InboxStoreType.InMemory).Returns(store);
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = options,
                CreateHandler = _ => DelegateHandler.Success(_ => tcs.TrySetResult()),
            };
            var worker = new InboxRetryWorker([subscription], storeFactory, Substitute.For<IServiceProvider>());

            await worker.StartAsync();
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await worker.StopAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(InboxMessageStatus.Processed, (await store.GetAsync("m1", ChannelKey, null))!.Status);
        }

        [Fact]
        public async Task StartAsync_StoreInitializationNotReady_ShouldThrowAndNeverStartPolling()
        {
            var store = Substitute.For<IInboxStore, IInboxStoreInitializer>();
            ((IInboxStoreInitializer)store).InitializeAsync(Arg.Any<StoreInitializationMode>(), Arg.Any<CancellationToken>())
                .Returns(new StoreInitializationResult
                {
                    Outcome = StoreInitializationOutcome.IncompatibleVersion,
                    RequiredSchemaVersion = 1,
                    PersistedSchemaVersion = 2,
                    Message = "too new",
                });
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore(StoreName, InboxStoreType.InMemory).Returns(store);
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = RetryOptions(),
                CreateHandler = _ => DelegateHandler.Success(_ => { }),
            };
            var worker = new InboxRetryWorker([subscription], storeFactory, Substitute.For<IServiceProvider>());

            var exception = await Assert.ThrowsAsync<InboxStoreInitializationFailedException>(() => worker.StartAsync());

            Assert.Equal(ChannelKey, exception.ChannelKey);
            Assert.Equal(StoreInitializationOutcome.IncompatibleVersion, exception.Result.Outcome);
            await store.DidNotReceiveWithAnyArgs().QueryRetryableAsync(default, default);
        }

        [Fact]
        public async Task StartAsync_StoreInitializationReady_ShouldInitializeBeforePolling()
        {
            var store = Substitute.For<IInboxStore, IInboxStoreInitializer>();
            ((IInboxStoreInitializer)store).InitializeAsync(Arg.Any<StoreInitializationMode>(), Arg.Any<CancellationToken>())
                .Returns(new StoreInitializationResult { Outcome = StoreInitializationOutcome.Ready, RequiredSchemaVersion = 1, PersistedSchemaVersion = 1, Message = "ok" });
            store.QueryRetryableAsync(Arg.Any<Guid>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore(StoreName, InboxStoreType.InMemory).Returns(store);
            var options = RetryOptions() with { RetryPollingInterval = TimeSpan.FromMilliseconds(10) };
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = options,
                CreateHandler = _ => DelegateHandler.Success(_ => { }),
            };
            var worker = new InboxRetryWorker([subscription], storeFactory, Substitute.For<IServiceProvider>());

            await worker.StartAsync();
            await worker.StopAsync(TimeSpan.FromSeconds(5));

            await ((IInboxStoreInitializer)store).Received(1).InitializeAsync(Arg.Any<StoreInitializationMode>(), Arg.Any<CancellationToken>());
        }

        private static InboxRetryWorker BuildWorker(
            ReferenceInboxStore store, TimeProvider timeProvider, IInboxRetryHandler handler, InboxOptions? options = null, Random? random = null)
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore(StoreName, InboxStoreType.InMemory).Returns(store);
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = options ?? RetryOptions(),
                CreateHandler = _ => handler,
            };

            return new InboxRetryWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, random ?? new FixedRandom(0.5));
        }

        private static async Task FailOnce(ReferenceInboxStore store, ManualTimeProvider timeProvider, string messageId)
        {
            var claim = await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = messageId,
                ChannelKey = ChannelKey,
                FeederId = FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
                LeaseOwner = "seed-owner",
                LeaseDuration = TimeSpan.FromMinutes(5),
            });
            await store.FailAsync(claim.Message!.Id, "seed-owner", "seed-failure", timeProvider.GetUtcNow() - TimeSpan.FromSeconds(1));
        }

        private static InboxOptions RetryOptions() => new()
        {
            InboxEnabled = true,
            StoreType = InboxStoreType.InMemory,
            StoreConnectionName = StoreName,
            RetryBatchSize = 10,
            RetryBaseDelay = TimeSpan.FromSeconds(1),
            RetryMaxDelay = TimeSpan.FromMinutes(5),
        };

        private sealed class FixedRandom(double value) : Random
        {
            public override double NextDouble() => value;
        }

        private sealed class DelegateHandler(Func<InboxMessage, ValueTask> onHandle) : IInboxRetryHandler
        {
            public ValueTask HandleAsync(InboxMessage message, CancellationToken cancellationToken) => onHandle(message);

            public static DelegateHandler Success(Action<InboxMessage> onHandle) =>
                new(message => { onHandle(message); return ValueTask.CompletedTask; });

            public static DelegateHandler Throwing(Exception exception) =>
                new(_ => throw exception);
        }

        private sealed class DelegateDeadLetterHandler(Func<InboxMessage, string, CancellationToken, ValueTask> onHandle) : IInboxDeadLetterHandler
        {
            public ValueTask HandleAsync(InboxMessage message, string failureReason, CancellationToken cancellationToken) =>
                onHandle(message, failureReason, cancellationToken);
        }
    }
}
