using Microsoft.Extensions.Logging.Testing;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxLogDeadLetterHandlerTests
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
            FailureReason = "System.InvalidOperationException",
            Payload = [1, 2, 3],
        };

        [Fact]
        public async Task HandleAsync_ShouldLogStableIdentifiersAndTheSanitizedFailureReason()
        {
            var logger = new FakeLogger<InboxLogDeadLetterHandler>();
            var handler = new InboxLogDeadLetterHandler(logger);
            var context = CreateContext();

            var outcome = await handler.HandleAsync(context, default);

            Assert.Equal(InboxDeadLetterHandlerOutcome.Handled, outcome);
            var record = Assert.Single(logger.Collector.GetSnapshot());
            Assert.Contains(context.Id.ToString(), record.Message);
            Assert.Contains(context.MessageId, record.Message);
            Assert.Contains(context.FailureReason, record.Message);
            Assert.Contains(context.FailureCategory.ToString(), record.Message);
        }

        [Fact]
        public async Task HandleAsync_ShouldNeverLogThePayload()
        {
            var logger = new FakeLogger<InboxLogDeadLetterHandler>();
            var handler = new InboxLogDeadLetterHandler(logger);

            await handler.HandleAsync(CreateContext(), default);

            var record = Assert.Single(logger.Collector.GetSnapshot());
            Assert.DoesNotContain("1, 2, 3", record.Message);
        }
    }

    public class InboxChannelDeadLetterHandlerTests
    {
        private sealed class RecordingSink : IInboxDeadLetterNotificationSink
        {
            public int CallCount { get; private set; }

            public Task NotifyAsync(InboxDeadLetterContext context, CancellationToken cancellationToken)
            {
                CallCount++;
                return Task.CompletedTask;
            }
        }

        private static InboxDeadLetterContext CreateContext(IReadOnlyDictionary<string, string>? headers = null) => new()
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
            Headers = headers ?? new Dictionary<string, string>(),
        };

        [Fact]
        public async Task HandleAsync_ShouldNotifyExactlyOnceWhenNoRecursionGuardHeaderIsPresent()
        {
            var sink = new RecordingSink();
            var handler = new InboxChannelDeadLetterHandler(sink, new FakeLogger<InboxChannelDeadLetterHandler>());

            var outcome = await handler.HandleAsync(CreateContext(), default);

            Assert.Equal(InboxDeadLetterHandlerOutcome.Handled, outcome);
            Assert.Equal(1, sink.CallCount);
        }

        [Fact]
        public async Task HandleAsync_ShouldSkipNotifyingAndReturnSkipped_WhenTheEntryIsItselfAnOutgoingNotification()
        {
            var sink = new RecordingSink();
            var handler = new InboxChannelDeadLetterHandler(sink, new FakeLogger<InboxChannelDeadLetterHandler>());
            var context = CreateContext(new Dictionary<string, string> { [InboxChannelDeadLetterHandler.RecursionGuardHeader] = "true" });

            var outcome = await handler.HandleAsync(context, default);

            Assert.Equal(InboxDeadLetterHandlerOutcome.Skipped, outcome);
            Assert.Equal(0, sink.CallCount);
        }
    }

    public class InboxBrokerDeadLetterHandlerTests
    {
        private sealed class RecordingPublisher(Func<int, Task>? onPublish = null) : IInboxDeadLetterBrokerPublisher
        {
            public int CallCount { get; private set; }
            public byte[]? LastPayload { get; private set; }
            public IReadOnlyDictionary<string, string>? LastHeaders { get; private set; }

            public async Task PublishAsync(InboxDeadLetterContext context, byte[] payload, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                CallCount++;
                LastPayload = payload;
                LastHeaders = headers;
                if (onPublish is not null)
                    await onPublish(CallCount).ConfigureAwait(false);
            }
        }

        private static InboxDeadLetterContext CreateContext(InboxDeadLetterPayloadPolicy payloadPolicy = InboxDeadLetterPayloadPolicy.Include, IReadOnlyDictionary<string, string>? headers = null) => new()
        {
            Id = Guid.NewGuid(),
            MessageId = "message-1",
            ChannelKey = Guid.NewGuid(),
            FeederId = Guid.NewGuid(),
            AttemptCount = 2,
            ReceivedAtUtc = DateTimeOffset.UnixEpoch,
            DeadLetteredAtUtc = DateTimeOffset.UnixEpoch,
            FailureCategory = InboxDeadLetterFailureCategory.Transient,
            FailureReason = "boom",
            TraceId = "trace-1",
            Payload = payloadPolicy == InboxDeadLetterPayloadPolicy.Include ? [1, 2, 3] : null,
            PayloadContentType = payloadPolicy != InboxDeadLetterPayloadPolicy.Omit ? "application/json" : null,
            Headers = headers ?? new Dictionary<string, string> { ["original"] = "value" },
        };

        [Fact]
        public async Task HandleAsync_ShouldPublishOnceWithStableMessageIdAndOriginalMetadata()
        {
            var publisher = new RecordingPublisher();
            var handler = new InboxBrokerDeadLetterHandler(publisher, retryDelay: TimeSpan.Zero);
            var context = CreateContext();

            var outcome = await handler.HandleAsync(context, default);

            Assert.Equal(InboxDeadLetterHandlerOutcome.Handled, outcome);
            Assert.Equal(1, publisher.CallCount);
            Assert.Equal(context.Payload, publisher.LastPayload);
            Assert.Equal("value", publisher.LastHeaders!["original"]);
            Assert.Equal(context.MessageId, publisher.LastHeaders[InboxBrokerDeadLetterHandler.MessageIdHeader]);
            Assert.Equal(context.FailureCategory.ToString(), publisher.LastHeaders[InboxBrokerDeadLetterHandler.FailureCategoryHeader]);
            Assert.Equal(context.FailureReason, publisher.LastHeaders[InboxBrokerDeadLetterHandler.FailureReasonHeader]);
            Assert.Equal(context.TraceId, publisher.LastHeaders[InboxBrokerDeadLetterHandler.TraceIdHeader]);
        }

        [Fact]
        public async Task HandleAsync_ShouldApplyThePayloadTransformBeforePublishing()
        {
            var publisher = new RecordingPublisher();
            var handler = new InboxBrokerDeadLetterHandler(publisher, retryDelay: TimeSpan.Zero, payloadTransform: bytes => bytes.Reverse().ToArray());

            await handler.HandleAsync(CreateContext(), default);

            Assert.Equal(new byte[] { 3, 2, 1 }, publisher.LastPayload);
        }

        [Theory]
        [InlineData(InboxDeadLetterPayloadPolicy.Redact)]
        [InlineData(InboxDeadLetterPayloadPolicy.Omit)]
        public async Task HandleAsync_ShouldThrowWithoutPublishing_WhenThePayloadWasNotCaptured(InboxDeadLetterPayloadPolicy policy)
        {
            var publisher = new RecordingPublisher();
            var handler = new InboxBrokerDeadLetterHandler(publisher, retryDelay: TimeSpan.Zero);

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
            var handler = new InboxBrokerDeadLetterHandler(publisher, maxPublishAttempts: 3, retryDelay: TimeSpan.Zero);

            var outcome = await handler.HandleAsync(CreateContext(), default);

            Assert.Equal(InboxDeadLetterHandlerOutcome.Handled, outcome);
            Assert.Equal(3, publisher.CallCount);
        }

        [Fact]
        public async Task HandleAsync_ShouldThrowAfterExhaustingTheBoundedRetries()
        {
            var publisher = new RecordingPublisher(_ => throw new InvalidOperationException("permanent"));
            var handler = new InboxBrokerDeadLetterHandler(publisher, maxPublishAttempts: 3, retryDelay: TimeSpan.Zero);

            var exception = await Assert.ThrowsAsync<InboxDeadLetterBrokerPublishException>(() => handler.HandleAsync(CreateContext(), default).AsTask());

            Assert.Equal(3, publisher.CallCount);
            Assert.IsType<InvalidOperationException>(exception.InnerException);
        }

        [Fact]
        public void Constructor_ShouldRejectANonPositiveMaxPublishAttempts()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new InboxBrokerDeadLetterHandler(new RecordingPublisher(), maxPublishAttempts: 0));
        }
    }

    public class InboxDeadLetterHandlerCompositionTests
    {
        private sealed class ThrowingBrokerPublisher : IInboxDeadLetterBrokerPublisher
        {
            public Task PublishAsync(InboxDeadLetterContext context, byte[] payload, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken) =>
                throw new InvalidOperationException("DLQ unreachable");
        }

        private sealed class RecordingNotificationSink : IInboxDeadLetterNotificationSink
        {
            public int CallCount { get; private set; }

            public Task NotifyAsync(InboxDeadLetterContext context, CancellationToken cancellationToken)
            {
                CallCount++;
                return Task.CompletedTask;
            }
        }

        [Fact]
        public async Task RunAsync_ComposedLogChannelAndBrokerHandlers_BrokerFailureShouldNotPreventTheOthers()
        {
            var logLogger = new FakeLogger<InboxLogDeadLetterHandler>();
            var channelSink = new RecordingNotificationSink();
            var handlers = new List<IInboxDeadLetterHandler>
            {
                new InboxLogDeadLetterHandler(logLogger),
                new InboxChannelDeadLetterHandler(channelSink, new FakeLogger<InboxChannelDeadLetterHandler>()),
                new InboxBrokerDeadLetterHandler(new ThrowingBrokerPublisher(), maxPublishAttempts: 1, retryDelay: TimeSpan.Zero),
            };
            var pipeline = new InboxDeadLetterPipeline(handlers);
            var context = new InboxDeadLetterContext
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
                Payload = [1, 2, 3],
            };

            var result = await pipeline.RunAsync(context);

            Assert.False(result.AllSucceeded);
            Assert.Equal(3, result.HandlerResults.Count);
            Assert.True(result.HandlerResults[0].Succeeded);
            Assert.True(result.HandlerResults[1].Succeeded);
            Assert.False(result.HandlerResults[2].Succeeded);
            Assert.IsType<InboxDeadLetterBrokerPublishException>(result.HandlerResults[2].Error);
            Assert.Single(logLogger.Collector.GetSnapshot());
            Assert.Equal(1, channelSink.CallCount);
        }
    }
}
