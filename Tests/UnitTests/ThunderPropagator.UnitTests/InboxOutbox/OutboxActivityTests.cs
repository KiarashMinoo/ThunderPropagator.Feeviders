using System.Diagnostics;
using NSubstitute;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    // ProviderKey is per-instance (xUnit creates a fresh instance per test method), never a shared
    // literal - see OutboxTelemetryTests' remarks on why a shared literal would let concurrently-running
    // tests bleed into an ActivityListener/MeterListener capture, both of which observe process-wide.
    public class OutboxActivityTests
    {
        private readonly string ProviderKey = $"provider-{Guid.NewGuid():N}";
        private const string StoreName = "relay-store";
        private const string ActivitySourceName = "thunderpropagator.providers.dotnet.outbox";

        [Fact]
        public async Task Enqueue_ShouldStartOutboxEnqueueAndPersistItsOwnTraceParentOnTheStagedRequest()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var store = new ReferenceOutboxStore(new ManualTimeProvider(DateTimeOffset.UnixEpoch));
            var unitOfWork = new NonTransactionalOutboxUnitOfWork(store);

            unitOfWork.Enqueue(Request("m1"));
            var enqueued = await unitOfWork.CommitAsync();

            var activity = Assert.Single(ForThisProvider(capture, "outbox.enqueue"));
            Assert.Equal(ProviderKey, activity.GetTagItem("outbox.provider_key"));
            Assert.Equal("m1", activity.GetTagItem("outbox.message_id"));
            Assert.Equal(activity.Id, enqueued[0].Headers[OutboxHeaderNames.TraceParent]);
        }

        [Fact]
        public async Task RunOnceAsync_PublishSucceeds_ShouldStartOutboxRelayWithOkStatus()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Success());

            await worker.RunOnceAsync();

            var activity = Assert.Single(ForThisProvider(capture, "outbox.relay"));
            Assert.Equal(ActivityStatusCode.Ok, activity.Status);
            Assert.Equal("m1", activity.GetTagItem("outbox.message_id"));
        }

        [Fact]
        public async Task RunOnceAsync_PublishFails_ShouldSetErrorStatus()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(Request("m1"));
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Throwing(new InvalidOperationException("boom")));

            await worker.RunOnceAsync();

            var activity = Assert.Single(ForThisProvider(capture, "outbox.relay"));
            Assert.Equal(ActivityStatusCode.Error, activity.Status);
        }

        [Fact]
        public async Task RunOnceAsync_RelayOfAnEntryWithAPersistedTraceParent_ShouldLinkBackToItAndStampAFreshOneOnTheOutgoingMessage()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            var originalTraceId = ActivityTraceId.CreateRandom();
            var originalSpanId = ActivitySpanId.CreateRandom();
            var originalTraceParent = $"00-{originalTraceId}-{originalSpanId}-01";
            await store.EnqueueAsync(Request("m1") with { Headers = new Dictionary<string, string> { [OutboxHeaderNames.TraceParent] = originalTraceParent } });

            IReadOnlyDictionary<string, string>? outgoingHeaders = null;
            var worker = BuildWorker(store, timeProvider, DelegateProvider.Custom((_, headers) => { outgoingHeaders = headers; return Task.CompletedTask; }));

            await worker.RunOnceAsync();

            var activity = Assert.Single(ForThisProvider(capture, "outbox.relay"));
            var link = Assert.Single(activity.Links);
            Assert.Equal(originalTraceId, link.Context.TraceId);
            Assert.Equal(originalSpanId, link.Context.SpanId);

            Assert.NotNull(outgoingHeaders);
            Assert.Equal(activity.Id, outgoingHeaders![OutboxHeaderNames.TraceParent]);
            Assert.NotEqual(originalTraceParent, outgoingHeaders[OutboxHeaderNames.TraceParent]);
        }

        private List<Activity> ForThisProvider(ActivityCapture capture, string operationName) =>
            [.. capture.Activities.Where(a => a.OperationName == operationName && Equals(a.GetTagItem("outbox.provider_key"), ProviderKey))];

        private OutboxEnqueueRequest Request(string messageId) => new()
        {
            MessageId = messageId,
            ProviderKey = ProviderKey,
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1, 2, 3],
        };

        private OutboxRelayWorker BuildWorker(ReferenceOutboxStore store, TimeProvider timeProvider, IProvider provider)
        {
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            storeFactory.GetStore(StoreName, OutboxStoreType.InMemory).Returns(store);
            var subscription = new OutboxRelaySubscription
            {
                ProviderKey = ProviderKey,
                Options = new OutboxOptions
                {
                    OutboxEnabled = true,
                    StoreType = OutboxStoreType.InMemory,
                    StoreConnectionName = StoreName,
                    RelayBatchSize = 10,
                    RetryBaseDelay = TimeSpan.FromSeconds(1),
                    RetryMaxDelay = TimeSpan.FromMinutes(5),
                },
                ResolveProvider = _ => provider,
            };

            return new OutboxRelayWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider, new FixedRandom(0.5));
        }

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

            public static DelegateProvider Custom(Func<byte[], IReadOnlyDictionary<string, string>?, Task> onPublish) =>
                new((bytes, headers, _) => onPublish(bytes, headers));
        }
    }
}
