using Microsoft.Extensions.Logging.Testing;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxLogDeadLetterHandlerTests
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
            FailureReason = "System.InvalidOperationException",
            Payload = [1, 2, 3],
        };

        [Fact]
        public async Task HandleAsync_ShouldLogStableIdentifiersAndTheSanitizedFailureReason()
        {
            var logger = new FakeLogger<OutboxLogDeadLetterHandler>();
            var handler = new OutboxLogDeadLetterHandler(logger);
            var context = CreateContext();

            var outcome = await handler.HandleAsync(context, default);

            Assert.Equal(OutboxDeadLetterHandlerOutcome.Handled, outcome);
            var record = Assert.Single(logger.Collector.GetSnapshot());
            Assert.Contains(context.Id.ToString(), record.Message);
            Assert.Contains(context.MessageId, record.Message);
            Assert.Contains(context.FailureReason, record.Message);
            Assert.Contains(context.FailureCategory.ToString(), record.Message);
        }

        [Fact]
        public async Task HandleAsync_ShouldNeverLogThePayload()
        {
            var logger = new FakeLogger<OutboxLogDeadLetterHandler>();
            var handler = new OutboxLogDeadLetterHandler(logger);

            await handler.HandleAsync(CreateContext(), default);

            var record = Assert.Single(logger.Collector.GetSnapshot());
            Assert.DoesNotContain("1, 2, 3", record.Message);
        }
    }

    public class OutboxChannelDeadLetterHandlerTests
    {
        private sealed class RecordingSink : IOutboxDeadLetterNotificationSink
        {
            public int CallCount { get; private set; }

            public Task NotifyAsync(OutboxDeadLetterContext context, CancellationToken cancellationToken)
            {
                CallCount++;
                return Task.CompletedTask;
            }
        }

        private static OutboxDeadLetterContext CreateContext(IReadOnlyDictionary<string, string>? headers = null) => new()
        {
            Id = Guid.NewGuid(),
            MessageId = "message-1",
            ProviderKey = "provider-a",
            Attempts = 3,
            CreatedAtUtc = DateTimeOffset.UnixEpoch,
            DeadLetteredAtUtc = DateTimeOffset.UnixEpoch,
            FailureCategory = OutboxDeadLetterFailureCategory.Poison,
            FailureReason = "boom",
            Headers = headers ?? new Dictionary<string, string>(),
        };

        [Fact]
        public async Task HandleAsync_ShouldNotifyExactlyOnceWhenNoRecursionGuardHeaderIsPresent()
        {
            var sink = new RecordingSink();
            var handler = new OutboxChannelDeadLetterHandler(sink, new FakeLogger<OutboxChannelDeadLetterHandler>());

            var outcome = await handler.HandleAsync(CreateContext(), default);

            Assert.Equal(OutboxDeadLetterHandlerOutcome.Handled, outcome);
            Assert.Equal(1, sink.CallCount);
        }

        [Fact]
        public async Task HandleAsync_ShouldSkipNotifyingAndReturnSkipped_WhenTheEntryIsItselfAnOutgoingNotification()
        {
            var sink = new RecordingSink();
            var handler = new OutboxChannelDeadLetterHandler(sink, new FakeLogger<OutboxChannelDeadLetterHandler>());
            var context = CreateContext(new Dictionary<string, string> { [OutboxChannelDeadLetterHandler.RecursionGuardHeader] = "true" });

            var outcome = await handler.HandleAsync(context, default);

            Assert.Equal(OutboxDeadLetterHandlerOutcome.Skipped, outcome);
            Assert.Equal(0, sink.CallCount);
        }
    }

    public class OutboxBrokerDeadLetterHandlerTests
    {
        private sealed class RecordingPublisher(Func<int, Task>? onPublish = null) : IOutboxDeadLetterBrokerPublisher
        {
            public int CallCount { get; private set; }
            public byte[]? LastPayload { get; private set; }
            public IReadOnlyDictionary<string, string>? LastHeaders { get; private set; }

            public async Task PublishAsync(OutboxDeadLetterContext context, byte[] payload, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                CallCount++;
                LastPayload = payload;
                LastHeaders = headers;
                if (onPublish is not null)
                    await onPublish(CallCount).ConfigureAwait(false);
            }
        }

        private static OutboxDeadLetterContext CreateContext(OutboxDeadLetterPayloadPolicy payloadPolicy = OutboxDeadLetterPayloadPolicy.Include, IReadOnlyDictionary<string, string>? headers = null) => new()
        {
            Id = Guid.NewGuid(),
            MessageId = "message-1",
            ProviderKey = "provider-a",
            Attempts = 2,
            CreatedAtUtc = DateTimeOffset.UnixEpoch,
            DeadLetteredAtUtc = DateTimeOffset.UnixEpoch,
            FailureCategory = OutboxDeadLetterFailureCategory.Transient,
            FailureReason = "boom",
            TraceId = "trace-1",
            Payload = payloadPolicy == OutboxDeadLetterPayloadPolicy.Include ? [1, 2, 3] : null,
            PayloadContentType = payloadPolicy != OutboxDeadLetterPayloadPolicy.Omit ? "application/json" : null,
            Headers = headers ?? new Dictionary<string, string> { ["original"] = "value" },
        };

        [Fact]
        public async Task HandleAsync_ShouldPublishOnceWithStableMessageIdAndOriginalMetadata()
        {
            var publisher = new RecordingPublisher();
            var handler = new OutboxBrokerDeadLetterHandler(publisher, retryDelay: TimeSpan.Zero);
            var context = CreateContext();

            var outcome = await handler.HandleAsync(context, default);

            Assert.Equal(OutboxDeadLetterHandlerOutcome.Handled, outcome);
            Assert.Equal(1, publisher.CallCount);
            Assert.Equal(context.Payload, publisher.LastPayload);
            Assert.Equal("value", publisher.LastHeaders!["original"]);
            Assert.Equal(context.MessageId, publisher.LastHeaders[OutboxBrokerDeadLetterHandler.MessageIdHeader]);
            Assert.Equal(context.FailureCategory.ToString(), publisher.LastHeaders[OutboxBrokerDeadLetterHandler.FailureCategoryHeader]);
            Assert.Equal(context.FailureReason, publisher.LastHeaders[OutboxBrokerDeadLetterHandler.FailureReasonHeader]);
            Assert.Equal(context.TraceId, publisher.LastHeaders[OutboxBrokerDeadLetterHandler.TraceIdHeader]);
        }

        [Fact]
        public async Task HandleAsync_ShouldApplyThePayloadTransformBeforePublishing()
        {
            var publisher = new RecordingPublisher();
            var handler = new OutboxBrokerDeadLetterHandler(publisher, retryDelay: TimeSpan.Zero, payloadTransform: bytes => bytes.Reverse().ToArray());

            await handler.HandleAsync(CreateContext(), default);

            Assert.Equal(new byte[] { 3, 2, 1 }, publisher.LastPayload);
        }

        [Theory]
        [InlineData(OutboxDeadLetterPayloadPolicy.Redact)]
        [InlineData(OutboxDeadLetterPayloadPolicy.Omit)]
        public async Task HandleAsync_ShouldThrowWithoutPublishing_WhenThePayloadWasNotCaptured(OutboxDeadLetterPayloadPolicy policy)
        {
            var publisher = new RecordingPublisher();
            var handler = new OutboxBrokerDeadLetterHandler(publisher, retryDelay: TimeSpan.Zero);

            await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleAsync(CreateContext(policy), default).AsTask());

            Assert.Equal(0, publisher.CallCount);
        }

        [Fact]
        public async Task HandleAsync_ShouldRetryUpToTheBoundThenSucceed()
        {
            var publisher = new RecordingPublisher(count =>
            {
                if (count < 3)
                    throw new InvalidOperationException("transient");
                return Task.CompletedTask;
            });
            var handler = new OutboxBrokerDeadLetterHandler(publisher, maxPublishAttempts: 3, retryDelay: TimeSpan.Zero);

            var outcome = await handler.HandleAsync(CreateContext(), default);

            Assert.Equal(OutboxDeadLetterHandlerOutcome.Handled, outcome);
            Assert.Equal(3, publisher.CallCount);
        }

        [Fact]
        public async Task HandleAsync_ShouldThrowAfterExhaustingTheBoundedRetries()
        {
            var publisher = new RecordingPublisher(_ => throw new InvalidOperationException("permanent"));
            var handler = new OutboxBrokerDeadLetterHandler(publisher, maxPublishAttempts: 3, retryDelay: TimeSpan.Zero);

            var exception = await Assert.ThrowsAsync<OutboxDeadLetterBrokerPublishException>(() => handler.HandleAsync(CreateContext(), default).AsTask());

            Assert.Equal(3, publisher.CallCount);
            Assert.IsType<InvalidOperationException>(exception.InnerException);
        }

        [Fact]
        public void Constructor_ShouldRejectANonPositiveMaxPublishAttempts()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxBrokerDeadLetterHandler(new RecordingPublisher(), maxPublishAttempts: 0));
        }
    }

    public class OutboxDeadLetterHandlerCompositionTests
    {
        private sealed class ThrowingBrokerPublisher : IOutboxDeadLetterBrokerPublisher
        {
            public Task PublishAsync(OutboxDeadLetterContext context, byte[] payload, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken) =>
                throw new InvalidOperationException("DLQ unreachable");
        }

        private sealed class RecordingNotificationSink : IOutboxDeadLetterNotificationSink
        {
            public int CallCount { get; private set; }

            public Task NotifyAsync(OutboxDeadLetterContext context, CancellationToken cancellationToken)
            {
                CallCount++;
                return Task.CompletedTask;
            }
        }

        [Fact]
        public async Task RunAsync_ComposedLogChannelAndBrokerHandlers_BrokerFailureShouldNotPreventTheOthers()
        {
            var logLogger = new FakeLogger<OutboxLogDeadLetterHandler>();
            var channelSink = new RecordingNotificationSink();
            var handlers = new List<IOutboxDeadLetterHandler>
            {
                new OutboxLogDeadLetterHandler(logLogger),
                new OutboxChannelDeadLetterHandler(channelSink, new FakeLogger<OutboxChannelDeadLetterHandler>()),
                new OutboxBrokerDeadLetterHandler(new ThrowingBrokerPublisher(), maxPublishAttempts: 1, retryDelay: TimeSpan.Zero),
            };
            var pipeline = new OutboxDeadLetterPipeline(handlers);
            var context = new OutboxDeadLetterContext
            {
                Id = Guid.NewGuid(),
                MessageId = "message-1",
                ProviderKey = "provider-a",
                Attempts = 3,
                CreatedAtUtc = DateTimeOffset.UnixEpoch,
                DeadLetteredAtUtc = DateTimeOffset.UnixEpoch,
                FailureCategory = OutboxDeadLetterFailureCategory.Poison,
                FailureReason = "boom",
                Payload = [1, 2, 3],
            };

            var result = await pipeline.RunAsync(context);

            Assert.False(result.AllSucceeded);
            Assert.Equal(3, result.HandlerResults.Count);
            Assert.True(result.HandlerResults[0].Succeeded);
            Assert.True(result.HandlerResults[1].Succeeded);
            Assert.False(result.HandlerResults[2].Succeeded);
            Assert.IsType<OutboxDeadLetterBrokerPublishException>(result.HandlerResults[2].Error);
            Assert.Single(logLogger.Collector.GetSnapshot());
            Assert.Equal(1, channelSink.CallCount);
        }
    }
}
