using System.Text;
using Confluent.Kafka;
using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.Redis;
using ThunderPropagator.Feeders.SharedKernel;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// End-to-end proof for issue #135: a real Kafka broker + a real Redis-backed Inbox store durably
/// deduplicate redelivered records before handler side effects, survive a process restart, commit
/// the broker offset before the handler completes (not after), and correctly recover from a
/// handler failure - rather than silently losing or double-processing a message.
/// </summary>
/// <remarks>
/// Drives a real <see cref="IConsumer{TKey,TValue}"/> directly through
/// <see cref="InboxReceiveCoordinator"/>, mirroring exactly what <c>KafkaFeeder</c>'s
/// Inbox-enabled receive path does internally (broker-header ID from a synthesized offset header,
/// partition-scoped dedup, commit-immediately-after-claim) - rather than constructing the full
/// <c>KafkaFeeder</c>/<c>DelegativeFeeder</c> object graph. This matches this repo's own
/// established precedent (<c>RabbitMQInboxAcknowledgementTests</c>): test the coordinator +
/// transport-ack collaboration directly, not the whole feeder class.
/// </remarks>
[Trait("Category", "Integration")]
public sealed class KafkaInboxDuplicateDiscardTests(KafkaContainerFixture kafkaFixture, RedisContainerFixture redisFixture)
    : IClassFixture<KafkaContainerFixture>, IClassFixture<RedisContainerFixture>
{
    private const string KafkaOffsetHeader = "kafka-offset";

    [Fact]
    public async Task ConcurrentDuplicateDelivery_ShouldInvokeHandlerExactlyOnce()
    {
        var topic = $"topic-{Guid.NewGuid():N}";
        var channelKey = Guid.NewGuid();
        var redisKeyPrefix = Guid.NewGuid().ToString("N");

        await kafkaFixture.ProduceAsync(topic, "order-42", "payload"u8.ToArray());

        var (coordinatorA, _) = CreateCoordinator(channelKey, redisKeyPrefix);
        var (coordinatorB, _) = CreateCoordinator(channelKey, redisKeyPrefix);
        using var consumerA = CreateConsumer(topic, $"group-{Guid.NewGuid():N}");
        using var consumerB = CreateConsumer(topic, $"group-{Guid.NewGuid():N}");

        var handlerInvocations = 0;
        Task Handler(byte[] payload, CancellationToken ct)
        {
            Interlocked.Increment(ref handlerInvocations);
            return Task.CompletedTask;
        }

        var outcomes = await Task.WhenAll(
            PollUntilReceivedAsync(coordinatorA, consumerA, Handler, TimeSpan.FromSeconds(30)),
            PollUntilReceivedAsync(coordinatorB, consumerB, Handler, TimeSpan.FromSeconds(30)));

        Assert.Equal(1, handlerInvocations);
        Assert.Contains(InboxReceiveOutcome.Processed, outcomes);
        Assert.Contains(outcomes, o => o is InboxReceiveOutcome.Duplicate or InboxReceiveOutcome.InProgress);
    }

    [Fact]
    public async Task ProcessRestart_ShouldNotReprocessAnAlreadyCompletedMessage()
    {
        var topic = $"topic-{Guid.NewGuid():N}";
        var groupId = $"group-{Guid.NewGuid():N}";
        var channelKey = Guid.NewGuid();
        var redisKeyPrefix = Guid.NewGuid().ToString("N");

        await kafkaFixture.ProduceAsync(topic, "order-1", "payload"u8.ToArray());

        var firstInvocations = 0;
        var (coordinator1, _) = CreateCoordinator(channelKey, redisKeyPrefix);
        using (var consumer1 = CreateConsumer(topic, groupId))
        {
            var outcome = await PollUntilReceivedAsync(coordinator1, consumer1,
                (_, _) => { firstInvocations++; return Task.CompletedTask; }, TimeSpan.FromSeconds(30));
            Assert.Equal(InboxReceiveOutcome.Processed, outcome);
        }

        // Simulate a process restart: a brand-new coordinator and a brand-new consumer, same
        // consumer-group id (so Kafka resumes from the already-committed offset) and the same
        // Redis-backed Inbox store.
        var secondInvocations = 0;
        var (coordinator2, _) = CreateCoordinator(channelKey, redisKeyPrefix);
        using var consumer2 = CreateConsumer(topic, groupId);
        var stillAvailable = await PollOnceThroughInboxAsync(coordinator2, consumer2,
            (_, _) => { secondInvocations++; return Task.CompletedTask; }, TimeSpan.FromSeconds(5));

        Assert.Null(stillAvailable); // nothing left to consume - the offset was already committed
        Assert.Equal(1, firstInvocations);
        Assert.Equal(0, secondInvocations);
    }

    [Fact]
    public async Task AcknowledgementTiming_ShouldCommitTheOffsetBeforeTheHandlerCompletes()
    {
        var topic = $"topic-{Guid.NewGuid():N}";
        var groupId = $"group-{Guid.NewGuid():N}";
        var channelKey = Guid.NewGuid();
        var redisKeyPrefix = Guid.NewGuid().ToString("N");

        await kafkaFixture.ProduceAsync(topic, "order-2", "payload"u8.ToArray());

        var (coordinator, _) = CreateCoordinator(channelKey, redisKeyPrefix);
        using var consumer = CreateConsumer(topic, groupId);

        var releaseHandler = new TaskCompletionSource();
        var handlerEntered = new TaskCompletionSource();

        var receiveTask = PollUntilReceivedAsync(coordinator, consumer, async (_, _) =>
        {
            handlerEntered.TrySetResult();
            await releaseHandler.Task;
        }, TimeSpan.FromSeconds(30));

        await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        // The handler is still blocked, but the coordinator commits the Kafka offset immediately
        // after the durable claim succeeds - before invoking the handler. A second, independent
        // consumer in the same group starting now must see nothing left to consume, proving the
        // commit already happened rather than waiting for the handler to finish.
        using (var probe = CreateConsumer(topic, groupId))
        {
            var probeResult = probe.Consume(TimeSpan.FromSeconds(5));
            Assert.True(probeResult is null || probeResult.IsPartitionEOF);
        }

        releaseHandler.TrySetResult();
        var outcome = await receiveTask;
        Assert.Equal(InboxReceiveOutcome.Processed, outcome);
    }

    [Fact]
    public async Task HandlerFailureThenRetry_ShouldReachProcessed()
    {
        var topic = $"topic-{Guid.NewGuid():N}";
        var groupId = $"group-{Guid.NewGuid():N}";
        var channelKey = Guid.NewGuid();
        var redisKeyPrefix = Guid.NewGuid().ToString("N");

        await kafkaFixture.ProduceAsync(topic, "order-3", "payload"u8.ToArray());

        // A near-zero retry base delay keeps the manual retry below from needing to fake the
        // clock - determinism of the backoff curve itself is already covered hermetically by
        // InboxRetryBackoffTests/InboxRetryWorkerTests at the unit level.
        var (coordinator, store) = CreateCoordinator(channelKey, redisKeyPrefix, retryBaseDelay: TimeSpan.FromMilliseconds(1));
        using (var consumer = CreateConsumer(topic, groupId))
        {
            var outcome = await PollUntilReceivedAsync(coordinator, consumer,
                (_, _) => throw new InvalidOperationException("Simulated handler failure."),
                TimeSpan.FromSeconds(30));
            Assert.Equal(InboxReceiveOutcome.Failed, outcome);
        }

        var failed = await WaitForFailedEntryAsync(store, channelKey);
        Assert.Equal(InboxMessageStatus.Failed, failed.Status);
        Assert.NotNull(failed.NextRetryAtUtc);

        // Drive the retry manually via the same store the coordinator used, rather than the
        // (separately tested) InboxRetryWorker - proving the Kafka-sourced entry is itself
        // correctly retry-eligible and completable, which is the Kafka-specific concern here.
        await Task.Delay(TimeSpan.FromMilliseconds(50));
        var retryable = await store.QueryRetryableAsync(channelKey, maxCount: 10);
        var retryEntry = Assert.Single(retryable, m => m.Id == failed.Id);
        var reclaim = await store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = retryEntry.MessageId,
            ChannelKey = channelKey,
            FeederId = retryEntry.FeederId,
            PartitionKey = retryEntry.PartitionKey,
            SchemaVersion = retryEntry.SchemaVersion,
            PayloadContentType = retryEntry.PayloadContentType,
            Payload = retryEntry.Payload,
            Headers = retryEntry.Headers,
            LeaseOwner = "manual-retry",
            LeaseDuration = TimeSpan.FromMinutes(1),
        });
        Assert.Equal(InboxClaimOutcome.Claimed, reclaim.Outcome);

        var completed = await store.CompleteAsync(reclaim.Message!.Id, "manual-retry");
        Assert.NotNull(completed);
        Assert.Equal(InboxMessageStatus.Processed, completed!.Status);
    }

    private (InboxReceiveCoordinator Coordinator, IInboxStore Store) CreateCoordinator(Guid channelKey, string redisKeyPrefix, TimeSpan? retryBaseDelay = null)
    {
        var store = new RedisInboxStore(redisFixture.Multiplexer, redisKeyPrefix);
        var storeFactory = Substitute.For<IInboxStoreFactory>();
        storeFactory.GetStore("kafka-test-redis", InboxStoreType.Redis).Returns(store);

        var options = new InboxOptions
        {
            InboxEnabled = true,
            StoreType = InboxStoreType.Redis,
            StoreConnectionName = "kafka-test-redis",
            RetryBaseDelay = retryBaseDelay ?? TimeSpan.FromSeconds(1),
            MessageIdResolution = new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.BrokerHeader,
                HeaderName = KafkaOffsetHeader,
            },
        };

        return (new InboxReceiveCoordinator(options, channelKey, Guid.NewGuid(), storeFactory), store);
    }

    private IConsumer<string, byte[]> CreateConsumer(string topic, string groupId)
    {
        var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafkaFixture.BootstrapAddress,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe(topic);

        return consumer;
    }

    /// <summary>
    /// One poll cycle: consumes at most one record and, if any arrived, routes it through
    /// <see cref="InboxReceiveCoordinator.ReceiveAsync"/> exactly as <c>KafkaFeeder</c> does -
    /// broker-header ID from a synthesized offset header, topic+partition as the dedup
    /// <c>PartitionKey</c>, and a commit-on-claim <c>acknowledgeAsync</c>. Returns <see langword="null"/>
    /// when nothing was available to consume this cycle.
    /// </summary>
    private static async Task<InboxReceiveOutcome?> PollOnceThroughInboxAsync(
        InboxReceiveCoordinator coordinator,
        IConsumer<string, byte[]> consumer,
        Func<byte[], CancellationToken, Task> invokeHandlerAsync,
        TimeSpan pollTimeout)
    {
        var consumeResult = consumer.Consume(pollTimeout);
        if (consumeResult?.Message is null || consumeResult.IsPartitionEOF)
            return null;

        var headers = new Dictionary<string, string>();
        foreach (var header in consumeResult.Message.Headers)
            headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
        headers[KafkaOffsetHeader] = consumeResult.Offset.Value.ToString();
        var partitionKey = $"{consumeResult.Topic}#{consumeResult.Partition.Value}";

        return await coordinator.ReceiveAsync(
            consumeResult.Message.Value,
            "application/octet-stream",
            headers,
            partitionKey,
            invokeHandlerAsync: ct => new ValueTask(invokeHandlerAsync(consumeResult.Message.Value, ct)),
            acknowledgeAsync: _ =>
            {
                consumer.Commit(consumeResult);
                return ValueTask.CompletedTask;
            },
            CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task<InboxReceiveOutcome> PollUntilReceivedAsync(
        InboxReceiveCoordinator coordinator,
        IConsumer<string, byte[]> consumer,
        Func<byte[], CancellationToken, Task> invokeHandlerAsync,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var outcome = await PollOnceThroughInboxAsync(coordinator, consumer, invokeHandlerAsync, TimeSpan.FromSeconds(2));
            if (outcome is not null)
                return outcome.Value;
        }

        throw new TimeoutException("No record became available to receive within the timeout.");
    }

    private static async Task<InboxMessage> WaitForFailedEntryAsync(IInboxStore store, Guid channelKey)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var retryable = await store.QueryRetryableAsync(channelKey, maxCount: 10);
            var failed = retryable.FirstOrDefault(m => m.Status == InboxMessageStatus.Failed);
            if (failed is not null)
                return failed;

            await Task.Delay(TimeSpan.FromMilliseconds(200));
        }

        throw new TimeoutException("The Inbox entry never reached the Failed status in time.");
    }
}
