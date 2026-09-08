using System.Runtime.Serialization;
using System.Security;
using System.Text.Json;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxFailureClassifierTests
    {
        [Fact]
        public void Classify_NonRetryableException_ShouldAlwaysBePermanentRegardlessOfAttemptsExhausted()
        {
            Assert.Equal(OutboxDeadLetterFailureCategory.Permanent, OutboxFailureClassifier.Classify(new OutboxNonRetryableException("boom"), attemptsExhausted: false));
            Assert.Equal(OutboxDeadLetterFailureCategory.Permanent, OutboxFailureClassifier.Classify(new OutboxNonRetryableException("boom"), attemptsExhausted: true));
        }

        [Theory]
        [InlineData(typeof(UnauthorizedAccessException))]
        [InlineData(typeof(SecurityException))]
        public void Classify_AuthorizationExceptions_ShouldBeAuthorization(Type exceptionType)
        {
            var exception = (Exception)Activator.CreateInstance(exceptionType)!;

            Assert.Equal(OutboxDeadLetterFailureCategory.Authorization, OutboxFailureClassifier.Classify(exception, attemptsExhausted: true));
        }

        [Theory]
        [InlineData(typeof(FormatException))]
        [InlineData(typeof(SerializationException))]
        [InlineData(typeof(JsonException))]
        public void Classify_SerializationExceptions_ShouldBeSerialization(Type exceptionType)
        {
            var exception = (Exception)Activator.CreateInstance(exceptionType)!;

            Assert.Equal(OutboxDeadLetterFailureCategory.Serialization, OutboxFailureClassifier.Classify(exception, attemptsExhausted: true));
        }

        [Fact]
        public void Classify_AttemptsNotExhausted_ShouldBePermanent_ForAnOtherwiseUnclassifiedException()
        {
            Assert.Equal(OutboxDeadLetterFailureCategory.Permanent, OutboxFailureClassifier.Classify(new InvalidOperationException("boom"), attemptsExhausted: false));
        }

        [Theory]
        [InlineData(typeof(TimeoutException))]
        [InlineData(typeof(HttpRequestExceptionStub))]
        [InlineData(typeof(IOException))]
        public void Classify_TransientExceptions_WhenAttemptsExhausted_ShouldBeTransient(Type exceptionType)
        {
            var exception = (Exception)Activator.CreateInstance(exceptionType)!;

            Assert.Equal(OutboxDeadLetterFailureCategory.Transient, OutboxFailureClassifier.Classify(exception, attemptsExhausted: true));
        }

        [Fact]
        public void Classify_OperationCanceledException_WhenAttemptsExhausted_ShouldBeTransient()
        {
            Assert.Equal(OutboxDeadLetterFailureCategory.Transient, OutboxFailureClassifier.Classify(new OperationCanceledException(), attemptsExhausted: true));
        }

        [Fact]
        public void Classify_UnrecognizedException_WhenAttemptsExhausted_ShouldBePoison()
        {
            Assert.Equal(OutboxDeadLetterFailureCategory.Poison, OutboxFailureClassifier.Classify(new InvalidOperationException("boom"), attemptsExhausted: true));
        }

        [Fact]
        public void Classify_NullException_ShouldThrow()
        {
            Assert.Throws<ArgumentNullException>(() => OutboxFailureClassifier.Classify(null!, attemptsExhausted: true));
        }

        private sealed class HttpRequestExceptionStub() : System.Net.Http.HttpRequestException("boom");
    }

    public class OutboxDeadLetterContextFactoryTests
    {
        private static OutboxMessage CreateDeadLettered()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var pending = OutboxMessage.CreatePending(
                Guid.NewGuid(), "message-1", "provider-a", orderingSequence: 1,
                schemaVersion: 1, payloadContentType: "application/json", payload: [1, 2, 3],
                headers: new Dictionary<string, string> { ["k"] = "v" }, partitionKey: null, timeProvider: timeProvider);

            return pending
                .TryTransitionTo(OutboxMessageStatus.Publishing, timeProvider, leaseOwner: "worker-1")
                .TryTransitionTo(OutboxMessageStatus.DeadLettered, timeProvider, failureReason: "boom");
        }

        [Fact]
        public void Create_IncludePolicy_ShouldCarryThePayloadAndContentType()
        {
            var message = CreateDeadLettered();

            var context = OutboxDeadLetterContextFactory.Create(message, OutboxDeadLetterFailureCategory.Poison, OutboxDeadLetterPayloadPolicy.Include);

            Assert.Equal(message.Payload, context.Payload);
            Assert.Equal(message.PayloadContentType, context.PayloadContentType);
        }

        [Fact]
        public void Create_RedactPolicy_ShouldOmitThePayloadButKeepTheContentType()
        {
            var message = CreateDeadLettered();

            var context = OutboxDeadLetterContextFactory.Create(message, OutboxDeadLetterFailureCategory.Poison, OutboxDeadLetterPayloadPolicy.Redact);

            Assert.Null(context.Payload);
            Assert.Equal(message.PayloadContentType, context.PayloadContentType);
        }

        [Fact]
        public void Create_OmitPolicy_ShouldOmitBothThePayloadAndTheContentType()
        {
            var message = CreateDeadLettered();

            var context = OutboxDeadLetterContextFactory.Create(message, OutboxDeadLetterFailureCategory.Poison, OutboxDeadLetterPayloadPolicy.Omit);

            Assert.Null(context.Payload);
            Assert.Null(context.PayloadContentType);
        }

        [Fact]
        public void Create_ShouldCopyIdentityAndClassificationFieldsFromTheMessage()
        {
            var message = CreateDeadLettered();

            var context = OutboxDeadLetterContextFactory.Create(message, OutboxDeadLetterFailureCategory.Authorization);

            Assert.Equal(message.Id, context.Id);
            Assert.Equal(message.MessageId, context.MessageId);
            Assert.Equal(message.ProviderKey, context.ProviderKey);
            Assert.Equal(message.Attempts, context.Attempts);
            Assert.Equal(message.CreatedAtUtc, context.CreatedAtUtc);
            Assert.Equal(message.DeadLetteredAtUtc, context.DeadLetteredAtUtc);
            Assert.Equal(OutboxDeadLetterFailureCategory.Authorization, context.FailureCategory);
            Assert.Equal(message.FailureReason, context.FailureReason);
            Assert.Equal(message.Headers, context.Headers);
            Assert.Equal(message.ProviderKey, context.Target);
        }

        [Fact]
        public void Create_NullMessage_ShouldThrow()
        {
            Assert.Throws<ArgumentNullException>(() => OutboxDeadLetterContextFactory.Create(null!, OutboxDeadLetterFailureCategory.Poison));
        }
    }

    public class OutboxDeadLetterPipelineTests
    {
        private static OutboxDeadLetterContext CreateContext() => new()
        {
            Id = Guid.NewGuid(),
            MessageId = "message-1",
            ProviderKey = "provider-a",
            Attempts = 3,
            CreatedAtUtc = DateTimeOffset.UnixEpoch,
            DeadLetteredAtUtc = DateTimeOffset.UnixEpoch,
            FailureCategory = OutboxDeadLetterFailureCategory.Poison,
            FailureReason = "boom",
        };

        private sealed class RecordingHandler(Func<OutboxDeadLetterContext, CancellationToken, ValueTask<OutboxDeadLetterHandlerOutcome>> onHandle) : IOutboxDeadLetterHandler
        {
            public int InvocationCount { get; private set; }

            public ValueTask<OutboxDeadLetterHandlerOutcome> HandleAsync(OutboxDeadLetterContext context, CancellationToken cancellationToken)
            {
                InvocationCount++;
                return onHandle(context, cancellationToken);
            }
        }

        [Fact]
        public async Task RunAsync_ShouldInvokeHandlersInRegistrationOrder()
        {
            var order = new List<int>();
            var first = new RecordingHandler((_, _) => { order.Add(1); return ValueTask.FromResult(OutboxDeadLetterHandlerOutcome.Handled); });
            var second = new RecordingHandler((_, _) => { order.Add(2); return ValueTask.FromResult(OutboxDeadLetterHandlerOutcome.Handled); });
            var pipeline = new OutboxDeadLetterPipeline([first, second]);

            await pipeline.RunAsync(CreateContext());

            Assert.Equal([1, 2], order);
        }

        [Fact]
        public async Task RunAsync_AHandlerThrowing_ShouldNotPreventLaterHandlersFromRunning()
        {
            var failing = new RecordingHandler((_, _) => throw new InvalidOperationException("boom"));
            var succeeding = new RecordingHandler((_, _) => ValueTask.FromResult(OutboxDeadLetterHandlerOutcome.Handled));
            var pipeline = new OutboxDeadLetterPipeline([failing, succeeding]);

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
            var handler = new RecordingHandler((_, _) => ValueTask.FromResult(OutboxDeadLetterHandlerOutcome.Handled));
            var pipeline = new OutboxDeadLetterPipeline([handler]);

            var result = await pipeline.RunAsync(CreateContext());

            Assert.True(result.AllSucceeded);
        }

        [Fact]
        public async Task RunAsync_CancelledBeforeAHandlerStarts_ShouldSkipItEntirely()
        {
            using var cts = new CancellationTokenSource();
            var first = new RecordingHandler((_, _) => { cts.Cancel(); return ValueTask.FromResult(OutboxDeadLetterHandlerOutcome.Handled); });
            var second = new RecordingHandler((_, _) => ValueTask.FromResult(OutboxDeadLetterHandlerOutcome.Handled));
            var pipeline = new OutboxDeadLetterPipeline([first, second]);

            var result = await pipeline.RunAsync(CreateContext(), cts.Token);

            Assert.Equal(0, second.InvocationCount);
            Assert.Single(result.HandlerResults);
        }
    }

    public class OutboxDeadLetterReplayServiceTests
    {
        private sealed class StubAuthorizer(OutboxReplayAuthorizationResult result) : IOutboxReplayAuthorizer
        {
            public int CallCount { get; private set; }

            public Task<OutboxReplayAuthorizationResult> AuthorizeAsync(OutboxMessage message, string requestedBy, CancellationToken cancellationToken)
            {
                CallCount++;
                return Task.FromResult(result);
            }
        }

        private sealed class RecordingAuditSink : IOutboxReplayAuditSink
        {
            public List<OutboxReplayAuditEntry> Entries { get; } = [];

            public Task RecordAsync(OutboxReplayAuditEntry entry, CancellationToken cancellationToken)
            {
                Entries.Add(entry);
                return Task.CompletedTask;
            }
        }

        private static OutboxEnqueueRequest CreateRequest(string messageId) => new()
        {
            MessageId = messageId,
            ProviderKey = "provider-a",
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1, 2, 3],
        };

        [Fact]
        public async Task ReplayAsync_NotDeadLettered_ShouldAuditAndThrowWithoutCallingTheAuthorizerOrStore()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            var authorizer = new StubAuthorizer(OutboxReplayAuthorizationResult.Allow());
            var auditSink = new RecordingAuditSink();
            var service = new OutboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);
            var pending = await store.EnqueueAsync(CreateRequest("message-1"));

            await Assert.ThrowsAsync<OutboxDeadLetterReplayException>(() => service.ReplayAsync(pending, "alice"));

            Assert.Equal(0, authorizer.CallCount);
            Assert.Single(auditSink.Entries);
            Assert.False(auditSink.Entries[0].Authorized);
            Assert.False(auditSink.Entries[0].Succeeded);
        }

        [Fact]
        public async Task ReplayAsync_AuthorizationDenied_ShouldAuditAndThrowWithoutCallingTheStore()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("message-1"));
            var claimed = (await store.ClaimBatchAsync(null, 10, "worker-1", TimeSpan.FromMinutes(5)))[0];
            await store.MarkDeadLetterAsync(claimed.Id, "worker-1", "boom");
            var deadLettered = store.Peek("message-1");

            var authorizer = new StubAuthorizer(OutboxReplayAuthorizationResult.Deny("not allowed"));
            var auditSink = new RecordingAuditSink();
            var service = new OutboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);

            await Assert.ThrowsAsync<OutboxDeadLetterReplayException>(() => service.ReplayAsync(deadLettered, "alice"));

            Assert.Single(auditSink.Entries);
            Assert.False(auditSink.Entries[0].Authorized);
            Assert.Equal("not allowed", auditSink.Entries[0].DenialReason);
            Assert.False(auditSink.Entries[0].Succeeded);
        }

        [Fact]
        public async Task ReplayAsync_Authorized_ShouldReplayAndAuditSuccess()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("message-1"));
            var claimed = (await store.ClaimBatchAsync(null, 10, "worker-1", TimeSpan.FromMinutes(5)))[0];
            await store.MarkDeadLetterAsync(claimed.Id, "worker-1", "boom");
            var deadLettered = store.Peek("message-1");

            var authorizer = new StubAuthorizer(OutboxReplayAuthorizationResult.Allow());
            var auditSink = new RecordingAuditSink();
            var service = new OutboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);

            var replayed = await service.ReplayAsync(deadLettered, "alice");

            Assert.Equal(OutboxMessageStatus.Pending, replayed.Status);
            Assert.Equal(1, authorizer.CallCount);
            Assert.Single(auditSink.Entries);
            Assert.True(auditSink.Entries[0].Authorized);
            Assert.True(auditSink.Entries[0].Succeeded);
        }

        [Fact]
        public async Task ReplayAsync_StoreNoLongerDeadLettered_ShouldAuditFailureAndThrow()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceOutboxStore(timeProvider);
            await store.EnqueueAsync(CreateRequest("message-1"));
            var claimed = (await store.ClaimBatchAsync(null, 10, "worker-1", TimeSpan.FromMinutes(5)))[0];
            await store.MarkDeadLetterAsync(claimed.Id, "worker-1", "boom");
            var deadLettered = store.Peek("message-1");
            // The entry is already replayed once behind the caller's back - the service's own store call
            // must fail even though the caller's in-hand snapshot still says DeadLettered.
            await store.ReplayAsync(deadLettered.Id);

            var authorizer = new StubAuthorizer(OutboxReplayAuthorizationResult.Allow());
            var auditSink = new RecordingAuditSink();
            var service = new OutboxDeadLetterReplayService(store, authorizer, auditSink, timeProvider);

            await Assert.ThrowsAsync<OutboxDeadLetterReplayException>(() => service.ReplayAsync(deadLettered, "alice"));

            Assert.Single(auditSink.Entries);
            Assert.True(auditSink.Entries[0].Authorized);
            Assert.False(auditSink.Entries[0].Succeeded);
        }
    }
}
