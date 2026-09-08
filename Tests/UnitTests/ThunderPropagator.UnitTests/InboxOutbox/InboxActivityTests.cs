using System.Diagnostics;
using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    // ChannelKey/FeederId are per-instance (xUnit creates a fresh instance per test method), never
    // static - see InboxTelemetryTests' remarks on why a shared literal would let concurrently-running
    // tests bleed into an ActivityListener/MeterListener capture, both of which observe process-wide.
    public class InboxActivityTests
    {
        private static readonly byte[] Payload = "payload"u8.ToArray();
        private readonly Guid ChannelKey = Guid.NewGuid();
        private readonly Guid FeederId = Guid.NewGuid();
        private const string ActivitySourceName = "thunderpropagator.feeders.inbox";

        [Fact]
        public async Task ReceiveAsync_NewClaim_ShouldStartInboxReceiveWithTagsAndOkStatus()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var claimed = BuildClaimedMessage();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(InboxClaimResult.Claimed(claimed));
            var coordinator = BuildCoordinator(EnabledOptions(), store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => ValueTask.CompletedTask, _ => ValueTask.CompletedTask);

            var activity = Assert.Single(ForThisChannel(capture, "inbox.receive"));
            Assert.Equal(ActivityStatusCode.Ok, activity.Status);
            Assert.Equal(ChannelKey.ToString(), activity.GetTagItem("inbox.channel_key")?.ToString());
            Assert.Equal(FeederId.ToString(), activity.GetTagItem("inbox.feeder_id")?.ToString());
            // Not claimed.MessageId: the default MessageIdResolution strategy (NewGuid) resolves its own
            // fresh identifier independent of whatever the mocked store hands back as the claimed entry.
            Assert.NotNull(activity.GetTagItem("inbox.message_id"));
        }

        [Fact]
        public async Task ReceiveAsync_HandlerThrows_ShouldSetErrorStatus()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var claimed = BuildClaimedMessage();
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>()).Returns(InboxClaimResult.Claimed(claimed));
            var coordinator = BuildCoordinator(EnabledOptions() with { MaxRetryAttempts = 5 }, store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => throw new InvalidOperationException("boom"), _ => ValueTask.CompletedTask);

            var activity = Assert.Single(ForThisChannel(capture, "inbox.receive"));
            Assert.Equal(ActivityStatusCode.Error, activity.Status);
        }

        [Fact]
        public async Task ReceiveAsync_NewClaim_ShouldPersistTraceParentMatchingTheActivityOntoTheClaimRequest()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var claimed = BuildClaimedMessage();
            InboxClaimRequest? capturedRequest = null;
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Do<InboxClaimRequest>(r => capturedRequest = r), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.Claimed(claimed));
            var coordinator = BuildCoordinator(EnabledOptions(), store);

            await coordinator.ReceiveAsync(Payload, "application/octet-stream", null, null,
                _ => ValueTask.CompletedTask, _ => ValueTask.CompletedTask);

            var activity = Assert.Single(ForThisChannel(capture, "inbox.receive"));
            Assert.NotNull(capturedRequest);
            Assert.True(capturedRequest!.Headers!.TryGetValue(InboxHeaderNames.TraceParent, out var traceParent));
            Assert.Equal(activity.Id, traceParent);
        }

        [Fact]
        public async Task RunOnceAsync_RetryOfAnEntryWithAPersistedTraceParent_ShouldLinkBackToIt()
        {
            using var capture = new ActivityCapture(ActivitySourceName);
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var originalTraceId = ActivityTraceId.CreateRandom();
            var originalSpanId = ActivitySpanId.CreateRandom();
            var originalTraceParent = $"00-{originalTraceId}-{originalSpanId}-01";

            var claim = await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = "m1",
                ChannelKey = ChannelKey,
                FeederId = FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/octet-stream",
                Payload = Payload,
                Headers = new Dictionary<string, string> { [InboxHeaderNames.TraceParent] = originalTraceParent },
                LeaseOwner = "owner-a",
                LeaseDuration = TimeSpan.FromMinutes(5),
            });
            await store.FailAsync(claim.Message!.Id, "owner-a", "boom", timeProvider.GetUtcNow() - TimeSpan.FromSeconds(1));

            var worker = BuildRetryWorker(store, timeProvider, DelegateHandler.Success());

            await worker.RunOnceAsync();

            var activity = Assert.Single(ForThisChannel(capture, "inbox.retry"));
            var link = Assert.Single(activity.Links);
            Assert.Equal(originalTraceId, link.Context.TraceId);
            Assert.Equal(originalSpanId, link.Context.SpanId);
        }

        private List<Activity> ForThisChannel(ActivityCapture capture, string operationName) =>
            [.. capture.Activities.Where(a => a.OperationName == operationName && Equals(a.GetTagItem("inbox.channel_key")?.ToString(), ChannelKey.ToString()))];

        private InboxReceiveCoordinator BuildCoordinator(InboxOptions options, IInboxStore store)
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            if (options.InboxEnabled)
                storeFactory.GetStore(options.StoreConnectionName!, options.StoreType).Returns(store);

            return new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
        }

        private InboxRetryWorker BuildRetryWorker(ReferenceInboxStore store, TimeProvider timeProvider, IInboxRetryHandler handler)
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            storeFactory.GetStore("retry-store", InboxStoreType.InMemory).Returns(store);
            var subscription = new InboxRetrySubscription
            {
                ChannelKey = ChannelKey,
                Options = EnabledOptions() with { StoreConnectionName = "retry-store", RetryBaseDelay = TimeSpan.FromSeconds(1), RetryMaxDelay = TimeSpan.FromMinutes(5) },
                CreateHandler = _ => handler,
            };

            return new InboxRetryWorker([subscription], storeFactory, Substitute.For<IServiceProvider>(), timeProvider);
        }

        private static InboxOptions EnabledOptions() => new()
        {
            InboxEnabled = true,
            StoreType = InboxStoreType.InMemory,
            StoreConnectionName = "test-store",
        };

        private InboxMessage BuildClaimedMessage(int attemptCount = 1) =>
            InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-id", ChannelKey, FeederId,
                schemaVersion: 1, payloadContentType: "application/octet-stream", payload: Payload,
                headers: null, partitionKey: null, timeProvider: TimeProvider.System)
            .TryTransitionTo(InboxMessageStatus.Processing, TimeProvider.System, leaseOwner: "owner", leaseExpiresAtUtc: DateTimeOffset.UtcNow.AddSeconds(30), incrementAttempt: true)
            with
            { AttemptCount = attemptCount };

        private sealed class DelegateHandler(Func<InboxMessage, ValueTask> onHandle) : IInboxRetryHandler
        {
            public ValueTask HandleAsync(InboxMessage message, CancellationToken cancellationToken) => onHandle(message);

            public static DelegateHandler Success() => new(_ => ValueTask.CompletedTask);
        }
    }
}
