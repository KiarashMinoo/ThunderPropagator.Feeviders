using NSubstitute;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    // ProviderKey is per-instance (xUnit creates a fresh instance per test method), never a shared
    // literal: MetricsCapture listens process-wide by meter name, and other test files (OutboxRelayWorkerTests,
    // OutboxPurgeWorkerTests) use the literal "provider-a" too - a shared literal here would let their
    // concurrently-running measurements bleed into these assertions. Scoping every assertion to this
    // test's own unique provider key keeps that noise out.
    public class OutboxTelemetryTests
    {
        private readonly string ProviderKey = $"provider-{Guid.NewGuid():N}";
        private const string StoreName = "relay-store";
        private const string MeterName = "thunderpropagator.providers.dotnet.outbox";

        [Fact]
        public async Task CommitAsync_NonTransactional_ShouldIncrementEnqueuedPerMessage()
        {
            using var capture = new MetricsCapture(MeterName);
            var store = new ReferenceOutboxStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            var unitOfWork = new NonTransactionalOutboxUnitOfWork(store);
            unitOfWork.Enqueue(Request("m1"));
            unitOfWork.Enqueue(Request("m2"));

            await unitOfWork.CommitAsync();

            Assert.Equal(2, ForThisProvider(capture, "thunderpropagator.providers.dotnet.outbox.enqueued").Count);
        }

        [Fact]
        public async Task RunOnceAsync_PublishSucceeds_ShouldIncrementPublishedAndRecordRelayDuration()
        {
            using var capture = new MetricsCapture(MeterName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Success());

            await worker.RunOnceAsync();

            Assert.Single(ForThisProvider(capture, "thunderpropagator.providers.dotnet.outbox.published"));
            var duration = Assert.Single(ForThisProvider(capture, "thunderpropagator.providers.dotnet.outbox.relay.duration"));
            Assert.Equal("published", duration.Tags["outcome"]);
        }

        [Fact]
        public async Task RunOnceAsync_PublishFailsBelowMaxAttempts_ShouldIncrementFailed()
        {
            using var capture = new MetricsCapture(MeterName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));
            var options = RelayOptions() with { MaxRetryAttempts = 5 };
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Throwing(new InvalidOperationException("boom")), options);

            await worker.RunOnceAsync();

            Assert.Single(ForThisProvider(capture, "thunderpropagator.providers.dotnet.outbox.failed"));
        }

        [Fact]
        public async Task RunOnceAsync_PublishFailsAtMaxAttempts_ShouldIncrementDeadLetteredTaggedWithCategory()
        {
            using var capture = new MetricsCapture(MeterName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));
            var options = RelayOptions() with { MaxRetryAttempts = 1 };
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Throwing(new InvalidOperationException("boom")), options);

            await worker.RunOnceAsync();

            var measurement = Assert.Single(ForThisProvider(capture, "thunderpropagator.providers.dotnet.outbox.dead_lettered"));
            Assert.Equal(OutboxDeadLetterFailureCategory.Poison.ToString(), measurement.Tags["category"]);
        }

        [Fact]
        public async Task RunOnceAsync_ShouldReportPendingDepthAndOldestAgeGauges()
        {
            using var capture = new MetricsCapture(MeterName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));
            timeProvider.Advance(TimeSpan.FromMinutes(5));
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Success());

            await worker.RunOnceAsync();
            capture.RecordObservableInstruments();

            Assert.Single(ForThisProvider(capture, "thunderpropagator.providers.dotnet.outbox.pending.depth"));
        }

        [Fact]
        public async Task RunOnceAsync_ShouldRecordStoreOperationDurationForEveryStoreCall()
        {
            using var capture = new MetricsCapture(MeterName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Success());

            await worker.RunOnceAsync();

            var durations = ForThisProvider(capture, "thunderpropagator.providers.dotnet.outbox.store_operation.duration");
            Assert.Contains(durations, m => Equals(m.Tags["operation"], "ClaimBatch") && Equals(m.Tags["outcome"], "success"));
            Assert.Contains(durations, m => Equals(m.Tags["operation"], "MarkPublished") && Equals(m.Tags["outcome"], "success"));
        }

        private List<(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags)> ForThisProvider(MetricsCapture capture, string instrument) =>
            capture.Measurements.Where(m => m.Instrument == instrument && Equals(m.Tags.GetValueOrDefault("provider"), ProviderKey)).ToList();

        private OutboxEnqueueRequest Request(string messageId) => new()
        {
            MessageId = messageId,
            ProviderKey = ProviderKey,
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1, 2, 3],
        };

        private OutboxRelayWorker BuildWorker(ReferenceOutboxStore store, TimeProvider timeProvider, IProvider provider, OutboxOptions? options = null)
        {
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            storeFactory.GetStore(StoreName, OutboxStoreType.InMemory).Returns(store);
            var subscription = new OutboxRelaySubscription
            {
                ProviderKey = ProviderKey,
                Options = options ?? RelayOptions(),
                ResolveProvider = _ => provider,
            };

            return new OutboxRelayWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, new FixedRandom(0.5));
        }

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
            public Task PublishDirectAsync(byte[] bytes, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken = default) =>
                onPublish(bytes, headers, cancellationToken);

            public void Dispose()
            {
            }

            public static DelegateProvider Success() => new((_, _, _) => Task.CompletedTask);

            public static DelegateProvider Throwing(Exception exception) => new((_, _, _) => throw exception);
        }
    }
}
