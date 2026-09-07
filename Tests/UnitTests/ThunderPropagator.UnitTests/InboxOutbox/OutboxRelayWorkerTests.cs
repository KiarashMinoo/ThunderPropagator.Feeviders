using NSubstitute;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxRelayWorkerTests
    {
        private const string ProviderKey = "provider-a";
        private const string StoreName = "relay-store";

        [Fact]
        public async Task RunOnceAsync_PublishSucceeds_ShouldMarkPublished()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));

            var provider = DelegateProvider.Success();
            var worker = BuildWorker(store, timeProvider, provider);

            await worker.RunOnceAsync();

            Assert.Single(provider.Published);
            Assert.Equal(OutboxMessageStatus.Published, (await Get(store, "m1")).Status);
        }

        [Fact]
        public async Task RunOnceAsync_ShouldPassOriginalPayloadAndHeadersToPublishDirectAsync()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            var headers = new Dictionary<string, string> { ["traceparent"] = "00-abc-def-01" };
            await store.EnqueueAsync(Request("m1") with { Payload = [9, 8, 7], Headers = headers });

            var provider = DelegateProvider.Success();
            var worker = BuildWorker(store, timeProvider, provider);

            await worker.RunOnceAsync();

            var (bytes, capturedHeaders) = Assert.Single(provider.Published);
            Assert.Equal<byte>([9, 8, 7], bytes);
            Assert.Equal("00-abc-def-01", capturedHeaders!["traceparent"]);
        }

        [Fact]
        public async Task RunOnceAsync_PublishThrows_BelowMaxAttempts_ShouldFailWithComputedBackoff()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));

            var options = RelayOptions() with { MaxRetryAttempts = 5 };
            var random = new FixedRandom(1.0); // full jitter fraction => exact exponential ceiling
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Throwing(new InvalidOperationException("boom")), options, random);

            await worker.RunOnceAsync();

            var message = await Get(store, "m1");
            Assert.Equal(OutboxMessageStatus.Failed, message.Status);
            Assert.Equal(1, message.Attempts);
            var expectedDelay = OutboxRetryBackoff.Compute(options, attemptCount: 1, new FixedRandom(1.0));
            Assert.Equal(timeProvider.GetUtcNow() + expectedDelay, message.NextRetryAtUtc);
        }

        [Fact]
        public async Task RunOnceAsync_PublishThrows_AtMaxAttempts_ShouldDeadLetter()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));

            var options = RelayOptions() with { MaxRetryAttempts = 1 };
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Throwing(new InvalidOperationException("boom")), options);

            await worker.RunOnceAsync();

            Assert.Equal(OutboxMessageStatus.DeadLettered, (await Get(store, "m1")).Status);
        }

        [Fact]
        public async Task RunOnceAsync_NonRetryableException_ShouldDeadLetterImmediatelyRegardlessOfRemainingAttempts()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));

            var options = RelayOptions() with { MaxRetryAttempts = 50 };
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Throwing(new OutboxNonRetryableException("unrecoverable")), options);

            await worker.RunOnceAsync();

            Assert.Equal(OutboxMessageStatus.DeadLettered, (await Get(store, "m1")).Status);
        }

        [Fact]
        public async Task RunOnceAsync_FailureReason_ShouldNeverPersistTheExceptionMessage()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));

            var options = RelayOptions() with { MaxRetryAttempts = 5 };
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Throwing(new InvalidOperationException("payload had secret order-42")), options);

            await worker.RunOnceAsync();

            var message = await Get(store, "m1");
            Assert.DoesNotContain("secret", message.FailureReason);
            Assert.DoesNotContain("order-42", message.FailureReason);
            Assert.Contains(nameof(InvalidOperationException), message.FailureReason);
        }

        [Fact]
        public async Task RunOnceAsync_AbandonedPublishingLease_ShouldBeReclaimedAndRepublished()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            var options = RelayOptions() with { ClaimLeaseDuration = TimeSpan.FromMinutes(1) };
            await store.EnqueueAsync(Request("abandoned"));

            // Simulate a worker that claimed and then crashed - never Marked Published/Failed/DeadLettered.
            var abandonedClaim = await store.ClaimBatchAsync(null, 10, "crashed-worker", options.ClaimLeaseDuration);
            Assert.Equal(OutboxMessageStatus.Publishing, abandonedClaim[0].Status);

            timeProvider.Advance(TimeSpan.FromMinutes(2));

            var provider = DelegateProvider.Success();
            var worker = BuildWorker(store, timeProvider, provider, options);

            await worker.RunOnceAsync();

            Assert.Single(provider.Published);
            Assert.Equal(OutboxMessageStatus.Published, (await Get(store, "abandoned")).Status);
        }

        [Fact]
        public async Task RunOnceAsync_TwoWorkersRacingTheSamePartition_ShouldPublishExactlyOnce()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));

            var publishCount = 0;
            var provider = DelegateProvider.Success(() => Interlocked.Increment(ref publishCount));
            var workerA = BuildWorker(store, timeProvider, provider);
            var workerB = BuildWorker(store, timeProvider, provider);

            await Task.WhenAll(workerA.RunOnceAsync(), workerB.RunOnceAsync());

            Assert.Equal(1, publishCount);
            Assert.Equal(OutboxMessageStatus.Published, (await Get(store, "m1")).Status);
        }

        [Fact]
        public async Task RunOnceAsync_DeadLetter_ShouldInvokeTheRegisteredDeadLetterHandler()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));

            OutboxMessage? notified = null;
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            storeFactory.GetStore(StoreName, OutboxStoreType.InMemory).Returns(store);
            var options = RelayOptions() with { MaxRetryAttempts = 1 };
            var subscription = new OutboxRelaySubscription
            {
                ProviderKey = ProviderKey,
                Options = options,
                ResolveProvider = _ => DelegateProvider.Throwing(new OutboxNonRetryableException("unrecoverable")),
                CreateDeadLetterHandler = _ => new DelegateDeadLetterHandler((message, _, _) =>
                {
                    notified = message;
                    return ValueTask.CompletedTask;
                }),
            };
            var worker = new OutboxRelayWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, new FixedRandom(0.5));

            await worker.RunOnceAsync();

            Assert.NotNull(notified);
            Assert.Equal("m1", notified!.MessageId);
        }

        [Fact]
        public async Task RunOnceAsync_TransientFailureMidBatch_ShouldReleaseLaterEntriesInThatPartitionPreservingOrder()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1", "partition-a"));
            await store.EnqueueAsync(Request("m2", "partition-a"));
            await store.EnqueueAsync(Request("m3", "partition-a"));

            var published = new List<string>();
            var provider = DelegateProvider.Custom(bytes =>
            {
                var id = System.Text.Encoding.UTF8.GetString(bytes);
                if (id == "m1")
                    throw new InvalidOperationException("transient");
                published.Add(id);
                return Task.CompletedTask;
            });
            var options = RelayOptions() with { MaxRetryAttempts = 5 };
            var worker = BuildWorker(store, timeProvider, provider, options);

            await worker.RunOnceAsync();

            Assert.Empty(published); // m1 failed first in ordering-sequence order, so m2/m3 never published this cycle
            Assert.Equal(OutboxMessageStatus.Failed, (await Get(store, "m1")).Status);
            // m2/m3 were claimed alongside m1 but released back to Pending rather than published out of order.
            Assert.Equal(OutboxMessageStatus.Pending, (await Get(store, "m2")).Status);
            Assert.Equal(OutboxMessageStatus.Pending, (await Get(store, "m3")).Status);
        }

        [Fact]
        public async Task RunOnceAsync_DeadLetteredEntryMidBatch_ShouldContinuePublishingLaterEntriesInThatPartition()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1", "partition-a"));
            await store.EnqueueAsync(Request("m2", "partition-a"));

            var published = new List<string>();
            var provider = DelegateProvider.Custom(bytes =>
            {
                var id = System.Text.Encoding.UTF8.GetString(bytes);
                if (id == "m1")
                    throw new OutboxNonRetryableException("poison");
                published.Add(id);
                return Task.CompletedTask;
            });
            var worker = BuildWorker(store, timeProvider, provider);

            await worker.RunOnceAsync();

            Assert.Equal(OutboxMessageStatus.DeadLettered, (await Get(store, "m1")).Status);
            Assert.Equal(["m2"], published);
            Assert.Equal(OutboxMessageStatus.Published, (await Get(store, "m2")).Status);
        }

        [Fact]
        public async Task RunOnceAsync_DifferentPartitions_ShouldEachBePublished()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("a1", "partition-a"));
            await store.EnqueueAsync(Request("b1", "partition-b"));

            var provider = DelegateProvider.Success();
            var worker = BuildWorker(store, timeProvider, provider);

            await worker.RunOnceAsync();

            Assert.Equal(2, provider.Published.Count);
            Assert.Equal(OutboxMessageStatus.Published, (await Get(store, "a1")).Status);
            Assert.Equal(OutboxMessageStatus.Published, (await Get(store, "b1")).Status);
        }

        [Fact]
        public void Constructor_DuplicateProviderKey_ShouldThrow()
        {
            var options = RelayOptions();
            var subscription = new OutboxRelaySubscription { ProviderKey = ProviderKey, Options = options, ResolveProvider = _ => DelegateProvider.Success() };

            Assert.Throws<ArgumentException>(() => new OutboxRelayWorker(
                [subscription, subscription with { }], Substitute.For<IOutboxStoreFactory>(), Substitute.For<IServiceProvider>()));
        }

        [Fact]
        public void Constructor_EnabledWithoutStoreConnectionName_ShouldThrow()
        {
            var subscription = new OutboxRelaySubscription
            {
                ProviderKey = ProviderKey,
                Options = RelayOptions() with { StoreConnectionName = null, StoreType = OutboxStoreType.InMemory },
                ResolveProvider = _ => DelegateProvider.Success(),
            };

            Assert.Throws<InvalidOperationException>(() => new OutboxRelayWorker(
                [subscription], Substitute.For<IOutboxStoreFactory>(), Substitute.For<IServiceProvider>()));
        }

        [Fact]
        public void Constructor_DisabledSubscription_ShouldNeverResolveAStore()
        {
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            var subscription = new OutboxRelaySubscription
            {
                ProviderKey = ProviderKey,
                Options = new OutboxOptions(), // OutboxEnabled defaults to false
                ResolveProvider = _ => DelegateProvider.Success(),
            };

            _ = new OutboxRelayWorker([subscription], storeFactory, Substitute.For<IServiceProvider>());

            storeFactory.DidNotReceiveWithAnyArgs().GetStore(default!, default);
        }

        [Fact]
        public async Task StartAsync_ThenStopAsync_ShouldPollAtLeastOnceAndShutDownCleanly()
        {
            var store = new ReferenceOutboxStore(TimeProvider.System);
            await store.EnqueueAsync(Request("m1"));

            var tcs = new TaskCompletionSource();
            var options = RelayOptions() with { RelayPollingInterval = TimeSpan.FromMilliseconds(10) };
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            storeFactory.GetStore(StoreName, OutboxStoreType.InMemory).Returns(store);
            var subscription = new OutboxRelaySubscription
            {
                ProviderKey = ProviderKey,
                Options = options,
                ResolveProvider = _ => DelegateProvider.Success(() => tcs.TrySetResult()),
            };
            var worker = new OutboxRelayWorker([subscription], storeFactory, Substitute.For<IServiceProvider>());

            await worker.StartAsync();
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await worker.StopAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(OutboxMessageStatus.Published, (await Get(store, "m1")).Status);
        }

        private static OutboxRelayWorker BuildWorker(
            ReferenceOutboxStore store, TimeProvider timeProvider, IProvider provider, OutboxOptions? options = null, Random? random = null)
        {
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            storeFactory.GetStore(StoreName, OutboxStoreType.InMemory).Returns(store);
            var subscription = new OutboxRelaySubscription
            {
                ProviderKey = ProviderKey,
                Options = options ?? RelayOptions(),
                ResolveProvider = _ => provider,
            };

            return new OutboxRelayWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, random ?? new FixedRandom(0.5));
        }

        private static Task<OutboxMessage> Get(ReferenceOutboxStore store, string messageId) => Task.FromResult(store.Peek(messageId));

        private static OutboxEnqueueRequest Request(string messageId, string? partitionKey = null) =>
            new()
            {
                MessageId = messageId,
                ProviderKey = ProviderKey,
                PartitionKey = partitionKey,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = System.Text.Encoding.UTF8.GetBytes(messageId),
            };

        private static OutboxOptions RelayOptions() => new()
        {
            OutboxEnabled = true,
            StoreType = OutboxStoreType.InMemory,
            StoreConnectionName = StoreName,
            RelayBatchSize = 10,
            RetryBaseDelay = TimeSpan.FromSeconds(1),
            RetryMaxDelay = TimeSpan.FromMinutes(5),
        };

        private sealed class FixedRandom(double value) : Random
        {
            public override double NextDouble() => value;
        }

        private sealed class DelegateProvider(Func<byte[], IReadOnlyDictionary<string, string>?, CancellationToken, Task> onPublish) : IProvider
        {
            private readonly System.Collections.Concurrent.ConcurrentBag<(byte[] Bytes, IReadOnlyDictionary<string, string>? Headers)> _published = [];

            public IReadOnlyCollection<(byte[] Bytes, IReadOnlyDictionary<string, string>? Headers)> Published => _published;

            public Task PublishDirectAsync(byte[] bytes, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken = default)
            {
                _published.Add((bytes, headers));
                return onPublish(bytes, headers, cancellationToken);
            }

            public void Dispose()
            {
            }

            public static DelegateProvider Success(Action? onPublish = null) =>
                new((_, _, _) => { onPublish?.Invoke(); return Task.CompletedTask; });

            public static DelegateProvider Throwing(Exception exception) =>
                new((_, _, _) => throw exception);

            public static DelegateProvider Custom(Func<byte[], Task> onPublish) =>
                new((bytes, _, _) => onPublish(bytes));
        }

        private sealed class DelegateDeadLetterHandler(Func<OutboxMessage, string, CancellationToken, ValueTask> onHandle) : IOutboxDeadLetterHandler
        {
            public ValueTask HandleAsync(OutboxMessage message, string failureReason, CancellationToken cancellationToken) =>
                onHandle(message, failureReason, cancellationToken);
        }
    }
}
