using Microsoft.Extensions.Diagnostics.HealthChecks;
using MongoDB.Driver;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.MongoDB;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.MongoDB;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Covers issue #126's "claims and transitions are atomic" (concurrency), "provide DI and health
/// integration" (health), and "support Mongo transactions for outbox plus business documents ... when
/// deployment/topology allows" (transaction) acceptance criteria, beyond what the shared contract suites
/// already exercise, against a real MongoDB replica-set container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MongoStoreConcurrencyHealthAndTransactionTests(MongoContainerFixture fixture) : IClassFixture<MongoContainerFixture>
{
    private IMongoDatabase NewDatabase() => new MongoClient(fixture.ConnectionString).GetDatabase($"db{Guid.NewGuid():N}");

    [Fact]
    public async Task MongoInboxStore_ManyPollersRacingOneMessage_ShouldClaimExactlyOnce()
    {
        var store = new MongoInboxStore(NewDatabase());
        await store.EnsureIndexesAsync();
        var channelKey = Guid.NewGuid();
        var feederId = Guid.NewGuid();

        var attempts = Enumerable.Range(0, 20).Select(i => store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "shared-message",
            ChannelKey = channelKey,
            FeederId = feederId,
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1, 2, 3],
            LeaseOwner = $"worker-{i}",
            LeaseDuration = TimeSpan.FromMinutes(5),
        }));

        var results = await Task.WhenAll(attempts);

        Assert.Single(results, r => r.Outcome == InboxClaimOutcome.Claimed);
        Assert.Equal(19, results.Count(r => r.Outcome == InboxClaimOutcome.ClaimedByAnotherOwner));
    }

    [Fact]
    public async Task MongoOutboxStore_ManyPollersRacingOnePartition_ShouldNeverClaimTheSameEntryTwice()
    {
        var store = new MongoOutboxStore(NewDatabase());
        await store.EnsureIndexesAsync();
        for (var i = 0; i < 20; i++)
        {
            await store.EnqueueAsync(new OutboxEnqueueRequest
            {
                MessageId = $"m{i}",
                ProviderKey = "provider-a",
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1],
            });
        }

        var claims = Enumerable.Range(0, 20).Select(i => store.ClaimBatchAsync(null, maxCount: 3, $"worker-{i}", TimeSpan.FromMinutes(5)));
        var batches = await Task.WhenAll(claims);

        var claimedIds = batches.SelectMany(b => b.Select(m => m.Id)).ToArray();
        Assert.Equal(20, claimedIds.Length);
        Assert.Equal(20, claimedIds.Distinct().Count());
    }

    [Fact]
    public async Task MongoInboxStore_CheckHealthAsync_ShouldReportHealthyWhileConnected()
    {
        var store = new MongoInboxStore(NewDatabase());

        var result = await store.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task MongoOutboxStore_CheckHealthAsync_ShouldReportHealthyWhileConnected()
    {
        var store = new MongoOutboxStore(NewDatabase());

        var result = await store.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task MongoOutboxStore_EnqueueAsync_WithSession_ShouldNeverBeVisibleUntilTheSessionCommits()
    {
        var database = NewDatabase();
        var store = new MongoOutboxStore(database);
        await store.EnsureIndexesAsync();
        var client = database.Client;

        using var session = await client.StartSessionAsync();
        session.StartTransaction();

        var enqueued = await store.EnqueueAsync(new OutboxEnqueueRequest
        {
            MessageId = "enlisted-message",
            ProviderKey = "provider-a",
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1],
        }, session);

        // Not yet visible to a reader outside the transaction - the whole point of enlisting the
        // Outbox write with the caller's own business-document writes on the same session.
        var beforeCommit = await store.GetDepthAsync(null);
        Assert.Equal(0, beforeCommit);

        await session.CommitTransactionAsync();

        var afterCommit = await store.GetDepthAsync(null);
        Assert.Equal(1, afterCommit);
        Assert.Equal("enlisted-message", enqueued.MessageId);
    }

    [Fact]
    public async Task MongoOutboxStore_EnqueueAsync_WithAbortedSession_ShouldNeverPersist()
    {
        var database = NewDatabase();
        var store = new MongoOutboxStore(database);
        await store.EnsureIndexesAsync();
        var client = database.Client;

        using var session = await client.StartSessionAsync();
        session.StartTransaction();

        await store.EnqueueAsync(new OutboxEnqueueRequest
        {
            MessageId = "aborted-message",
            ProviderKey = "provider-a",
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1],
        }, session);

        await session.AbortTransactionAsync();

        var depth = await store.GetDepthAsync(null);
        Assert.Equal(0, depth);
    }
}
