using System.Runtime.Serialization;
using System.Security;
using System.Text.Json;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxFailureClassifierTests
    {
        [Fact]
        public void Classify_NonRetryableException_ShouldAlwaysBePermanentRegardlessOfAttemptsExhausted()
        {
            Assert.Equal(InboxDeadLetterFailureCategory.Permanent, InboxFailureClassifier.Classify(new InboxNonRetryableException("boom"), attemptsExhausted: false));
            Assert.Equal(InboxDeadLetterFailureCategory.Permanent, InboxFailureClassifier.Classify(new InboxNonRetryableException("boom"), attemptsExhausted: true));
        }

        [Theory]
        [InlineData(typeof(UnauthorizedAccessException))]
        [InlineData(typeof(SecurityException))]
        public void Classify_AuthorizationExceptions_ShouldBeAuthorization(Type exceptionType)
        {
            var exception = (Exception)Activator.CreateInstance(exceptionType)!;

            Assert.Equal(InboxDeadLetterFailureCategory.Authorization, InboxFailureClassifier.Classify(exception, attemptsExhausted: true));
        }

        [Theory]
        [InlineData(typeof(FormatException))]
        [InlineData(typeof(SerializationException))]
        [InlineData(typeof(JsonException))]
        public void Classify_SerializationExceptions_ShouldBeSerialization(Type exceptionType)
        {
            var exception = (Exception)Activator.CreateInstance(exceptionType)!;

            Assert.Equal(InboxDeadLetterFailureCategory.Serialization, InboxFailureClassifier.Classify(exception, attemptsExhausted: true));
        }

        [Fact]
        public void Classify_AttemptsNotExhausted_ShouldBePermanent_ForAnOtherwiseUnclassifiedException()
        {
            Assert.Equal(InboxDeadLetterFailureCategory.Permanent, InboxFailureClassifier.Classify(new InvalidOperationException("boom"), attemptsExhausted: false));
        }

        [Theory]
        [InlineData(typeof(TimeoutException))]
        [InlineData(typeof(HttpRequestExceptionStub))]
        [InlineData(typeof(IOException))]
        public void Classify_TransientExceptions_WhenAttemptsExhausted_ShouldBeTransient(Type exceptionType)
        {
            var exception = (Exception)Activator.CreateInstance(exceptionType)!;

            Assert.Equal(InboxDeadLetterFailureCategory.Transient, InboxFailureClassifier.Classify(exception, attemptsExhausted: true));
        }

        [Fact]
        public void Classify_OperationCanceledException_WhenAttemptsExhausted_ShouldBeTransient()
        {
            Assert.Equal(InboxDeadLetterFailureCategory.Transient, InboxFailureClassifier.Classify(new OperationCanceledException(), attemptsExhausted: true));
        }

        [Fact]
        public void Classify_UnrecognizedException_WhenAttemptsExhausted_ShouldBePoison()
        {
            Assert.Equal(InboxDeadLetterFailureCategory.Poison, InboxFailureClassifier.Classify(new InvalidOperationException("boom"), attemptsExhausted: true));
        }

        [Fact]
        public void Classify_NullException_ShouldThrow()
        {
            Assert.Throws<ArgumentNullException>(() => InboxFailureClassifier.Classify(null!, attemptsExhausted: true));
        }

        private sealed class HttpRequestExceptionStub() : System.Net.Http.HttpRequestException("boom");
    }

    public class InboxDeadLetterContextFactoryTests
    {
        private static InboxMessage CreateDeadLettered()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var received = InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-1", Guid.NewGuid(), Guid.NewGuid(),
                schemaVersion: 1, payloadContentType: "application/json", payload: [1, 2, 3],
                headers: new Dictionary<string, string> { ["k"] = "v" }, partitionKey: null, timeProvider: timeProvider);

            return received
                .TryTransitionTo(InboxMessageStatus.Processing, timeProvider, leaseOwner: "worker-1")
                .TryTransitionTo(InboxMessageStatus.DeadLettered, timeProvider, failureReason: "boom");
        }

        [Fact]
        public void Create_IncludePolicy_ShouldCarryThePayloadAndContentType()
        {
            var message = CreateDeadLettered();

            var context = InboxDeadLetterContextFactory.Create(message, InboxDeadLetterFailureCategory.Poison, InboxDeadLetterPayloadPolicy.Include);

            Assert.Equal(message.Payload, context.Payload);
            Assert.Equal(message.PayloadContentType, context.PayloadContentType);
        }

        [Fact]
        public void Create_RedactPolicy_ShouldOmitThePayloadButKeepTheContentType()
        {
            var message = CreateDeadLettered();

            var context = InboxDeadLetterContextFactory.Create(message, InboxDeadLetterFailureCategory.Poison, InboxDeadLetterPayloadPolicy.Redact);

            Assert.Null(context.Payload);
            Assert.Equal(message.PayloadContentType, context.PayloadContentType);
        }

        [Fact]
        public void Create_OmitPolicy_ShouldOmitBothThePayloadAndTheContentType()
        {
            var message = CreateDeadLettered();

            var context = InboxDeadLetterContextFactory.Create(message, InboxDeadLetterFailureCategory.Poison, InboxDeadLetterPayloadPolicy.Omit);

            Assert.Null(context.Payload);
            Assert.Null(context.PayloadContentType);
        }

        [Fact]
        public void Create_ShouldCopyIdentityAndClassificationFieldsFromTheMessage()
        {
            var message = CreateDeadLettered();

            var context = InboxDeadLetterContextFactory.Create(message, InboxDeadLetterFailureCategory.Authorization);

            Assert.Equal(message.Id, context.Id);
            Assert.Equal(message.MessageId, context.MessageId);
            Assert.Equal(message.ChannelKey, context.ChannelKey);
            Assert.Equal(message.FeederId, context.FeederId);
            Assert.Equal(message.AttemptCount, context.AttemptCount);
            Assert.Equal(message.ReceivedAtUtc, context.ReceivedAtUtc);
            Assert.Equal(message.DeadLetteredAtUtc, context.DeadLetteredAtUtc);
            Assert.Equal(InboxDeadLetterFailureCategory.Authorization, context.FailureCategory);
            Assert.Equal(message.FailureReason, context.FailureReason);
            Assert.Equal(message.Headers, context.Headers);
            Assert.Equal(message.FeederId.ToString(), context.Source);
            Assert.Equal(message.ChannelKey.ToString(), context.Target);
        }

        [Fact]
        public void Create_NullMessage_ShouldThrow()
        {
            Assert.Throws<ArgumentNullException>(() => InboxDeadLetterContextFactory.Create(null!, InboxDeadLetterFailureCategory.Poison));
        }
    }

    public class InboxDeadLetterPipelineTests
    {
        private static InboxDeadLetterContext CreateContext() => new()
        {
            Id = Guid.NewGuid(),
            MessageId = "message-1",
            ChannelKey = Guid.NewGuid(),
            FeederId = Guid.NewGuid(),
            AttemptCount = 3,
            ReceivedAtUtc = DateTimeOffset.UnixEpoch,
            DeadLetteredAtUtc = DateTimeOffset.UnixEpoch,
            FailureCategory = InboxDeadLetterFailureCategory.Poison,
            FailureReason = "boom",
        };

        private sealed class RecordingHandler(Func<InboxDeadLetterContext, CancellationToken, ValueTask<InboxDeadLetterHandlerOutcome>> onHandle) : IInboxDeadLetterHandler
        {
            public int InvocationCount { get; private set; }

            public ValueTask<InboxDeadLetterHandlerOutcome> HandleAsync(InboxDeadLetterContext context, CancellationToken cancellationToken)
            {
                InvocationCount++;
                return onHandle(context, cancellationToken);
            }
        }

        [Fact]
        public async Task RunAsync_ShouldInvokeHandlersInRegistrationOrder()
        {
            var order = new List<int>();
            var first = new RecordingHandler((_, _) => { order.Add(1); return ValueTask.FromResult(InboxDeadLetterHandlerOutcome.Handled); });
            var second = new RecordingHandler((_, _) => { order.Add(2); return ValueTask.FromResult(InboxDeadLetterHandlerOutcome.Handled); });
            var pipeline = new InboxDeadLetterPipeline([first, second]);

            await pipeline.RunAsync(CreateContext());

            Assert.Equal([1, 2], order);
        }

        [Fact]
        public async Task RunAsync_AHandlerThrowing_ShouldNotPreventLaterHandlersFromRunning()
        {
            var failing = new RecordingHandler((_, _) => throw new InvalidOperationException("boom"));
            var succeeding = new RecordingHandler((_, _) => ValueTask.FromResult(InboxDeadLetterHandlerOutcome.Handled));
            var pipeline = new InboxDeadLetterPipeline([failing, succeeding]);

            var result = await pipeline.RunAsync(CreateContext());

            Assert.Equal(1, succeeding.InvocationCount);
            Assert.False(result.AllSucceeded);
            Assert.Equal(2, result.HandlerResults.Count);
            Assert.False(result.HandlerResults[0].Succeeded);
            Assert.IsType<InvalidOperationException>(result.HandlerResults[0].Error);
            Assert.True(result.HandlerResults[1].Succeeded);
        }

        [Fact]
        public async Task RunAsync_AllHandlersSucceeding_ShouldReportAllSucceededTrue()
        {
            var handler = new RecordingHandler((_, _) => ValueTask.FromResult(InboxDeadLetterHandlerOutcome.Handled));
            var pipeline = new InboxDeadLetterPipeline([handler]);

            var result = await pipeline.RunAsync(CreateContext());

            Assert.True(result.AllSucceeded);
        }

        [Fact]
        public async Task RunAsync_CancelledBeforeAHandlerStarts_ShouldSkipItEntirely()
        {
            using var cts = new CancellationTokenSource();
            var first = new RecordingHandler((_, _) => { cts.Cancel(); return ValueTask.FromResult(InboxDeadLetterHandlerOutcome.Handled); });
            var second = new RecordingHandler((_, _) => ValueTask.FromResult(InboxDeadLetterHandlerOutcome.Handled));
            var pipeline = new InboxDeadLetterPipeline([first, second]);

            var result = await pipeline.RunAsync(CreateContext(), cts.Token);

            Assert.Equal(0, second.InvocationCount);
            Assert.Single(result.HandlerResults);
        }
    }

    public class InboxDeadLetterReplayServiceTests
    {
        private sealed class StubAuthorizer(InboxReplayAuthorizationResult result) : IInboxReplayAuthorizer
        {
            public int CallCount { get; private set; }

            public Task<InboxReplayAuthorizationResult> AuthorizeAsync(InboxMessage message, string requestedBy, CancellationToken cancellationToken)
            {
                CallCount++;
                return Task.FromResult(result);
            }
        }

        private sealed class RecordingAuditSink : IInboxReplayAuditSink
        {
            public List<InboxReplayAuditEntry> Entries { get; } = [];

            public Task RecordAsync(InboxReplayAuditEntry entry, CancellationToken cancellationToken)
            {
                Entries.Add(entry);
                return Task.CompletedTask;
            }
        }

        private static InboxMessage CreateDeadLettered(TimeProvider timeProvider)
        {
            var received = InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-1", Guid.NewGuid(), Guid.NewGuid(),
                schemaVersion: 1, payloadContentType: "application/json", payload: [1, 2, 3],
                headers: null, partitionKey: null, timeProvider: timeProvider);

            return received
                .TryTransitionTo(InboxMessageStatus.Processing, timeProvider, leaseOwner: "worker-1")
                .TryTransitionTo(InboxMessageStatus.DeadLettered, timeProvider, failureReason: "boom");
        }

        [Fact]
        public async Task ReplayAsync_NotDeadLettered_ShouldAuditAndThrowWithoutCallingTheAuthorizerOrStore()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var authorizer = new StubAuthorizer(InboxReplayAuthorizationResult.Allow());
            var auditSink = new RecordingAuditSink();
            var service = new InboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);
            var received = InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-1", Guid.NewGuid(), Guid.NewGuid(),
                schemaVersion: 1, payloadContentType: "application/json", payload: [1, 2, 3],
                headers: null, partitionKey: null, timeProvider: timeProvider);

            await Assert.ThrowsAsync<InboxDeadLetterReplayException>(() => service.ReplayAsync(received, "alice"));

            Assert.Equal(0, authorizer.CallCount);
            Assert.Single(auditSink.Entries);
            Assert.False(auditSink.Entries[0].Authorized);
            Assert.False(auditSink.Entries[0].Succeeded);
        }

        [Fact]
        public async Task ReplayAsync_AuthorizationDenied_ShouldAuditAndThrowWithoutCallingTheStore()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var deadLettered = CreateDeadLettered(timeProvider);
            var authorizer = new StubAuthorizer(InboxReplayAuthorizationResult.Deny("not allowed"));
            var auditSink = new RecordingAuditSink();
            var service = new InboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);

            await Assert.ThrowsAsync<InboxDeadLetterReplayException>(() => service.ReplayAsync(deadLettered, "alice"));

            Assert.Single(auditSink.Entries);
            Assert.False(auditSink.Entries[0].Authorized);
            Assert.Equal("not allowed", auditSink.Entries[0].DenialReason);
            Assert.False(auditSink.Entries[0].Succeeded);
        }

        [Fact]
        public async Task ReplayAsync_Authorized_ShouldReplayAndAuditSuccess()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var deadLettered = CreateDeadLettered(timeProvider);
            await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = deadLettered.MessageId,
                ChannelKey = deadLettered.ChannelKey,
                FeederId = deadLettered.FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
                LeaseOwner = "worker-1",
                LeaseDuration = TimeSpan.FromMinutes(5),
            });
            var claimed = await store.GetAsync(deadLettered.MessageId, deadLettered.ChannelKey, null);
            await store.DeadLetterAsync(claimed!.Id, "worker-1", "boom");
            var stored = (await store.GetAsync(deadLettered.MessageId, deadLettered.ChannelKey, null))!;

            var authorizer = new StubAuthorizer(InboxReplayAuthorizationResult.Allow());
            var auditSink = new RecordingAuditSink();
            var service = new InboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);

            var replayed = await service.ReplayAsync(stored, "alice");

            Assert.Equal(InboxMessageStatus.Received, replayed.Status);
            Assert.Equal(1, authorizer.CallCount);
            Assert.Single(auditSink.Entries);
            Assert.True(auditSink.Entries[0].Authorized);
            Assert.True(auditSink.Entries[0].Succeeded);
        }

        [Fact]
        public async Task ReplayAsync_StoreNoLongerDeadLettered_ShouldAuditFailureAndThrow()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var deadLettered = CreateDeadLettered(timeProvider);
            await store.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = deadLettered.MessageId,
                ChannelKey = deadLettered.ChannelKey,
                FeederId = deadLettered.FeederId,
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1, 2, 3],
                LeaseOwner = "worker-1",
                LeaseDuration = TimeSpan.FromMinutes(5),
            });
            var claimed = (await store.GetAsync(deadLettered.MessageId, deadLettered.ChannelKey, null))!;
            await store.DeadLetterAsync(claimed.Id, "worker-1", "boom");
            var stored = (await store.GetAsync(deadLettered.MessageId, deadLettered.ChannelKey, null))!;
            // The entry is already replayed once behind the caller's back - the service's own store call
            // must fail even though the caller's in-hand snapshot still says DeadLettered.
            await store.ReplayAsync(stored.Id);

            var authorizer = new StubAuthorizer(InboxReplayAuthorizationResult.Allow());
            var auditSink = new RecordingAuditSink();
            var service = new InboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);

            await Assert.ThrowsAsync<InboxDeadLetterReplayException>(() => service.ReplayAsync(stored, "alice"));

            Assert.Single(auditSink.Entries);
            Assert.True(auditSink.Entries[0].Authorized);
            Assert.False(auditSink.Entries[0].Succeeded);
        }
    }
}
