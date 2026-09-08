using MongoDB.Bson;
using MongoDB.Driver;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.MongoDB;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.MongoDB;
using Xunit;
using InboxMode = ThunderPropagator.Feeders.Inbox.StoreInitializationMode;
using InboxOutcome = ThunderPropagator.Feeders.Inbox.StoreInitializationOutcome;
using OutboxMode = ThunderPropagator.Providers.DotNet.Outbox.StoreInitializationMode;
using OutboxOutcome = ThunderPropagator.Providers.DotNet.Outbox.StoreInitializationOutcome;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Covers issue #127's "idempotent under concurrent replicas", "workers remain unready until required
/// schema is valid", and "external migration mode verifies schema without mutating it" acceptance
/// criteria for <see cref="MongoInboxStore.InitializeAsync"/>/<see cref="MongoOutboxStore.InitializeAsync"/>,
/// against a real MongoDB replica-set container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MongoStoreInitializationTests(MongoContainerFixture fixture) : IClassFixture<MongoContainerFixture>
{
    private IMongoDatabase NewDatabase() => new MongoClient(fixture.ConnectionString).GetDatabase($"db{Guid.NewGuid():N}");

    [Fact]
    public async Task MongoInboxStore_InitializeAsync_FirstStart_ShouldCreateSchemaAtRequiredVersion()
    {
        var store = new MongoInboxStore(NewDatabase());

        var result = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Upgraded, result.Outcome);
        Assert.True(result.IsReady);
        Assert.Null(result.PersistedSchemaVersion);

        var claim = await store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "m1", ChannelKey = Guid.NewGuid(), FeederId = Guid.NewGuid(), SchemaVersion = 1,
            PayloadContentType = "application/json", Payload = [1], LeaseOwner = "w1", LeaseDuration = TimeSpan.FromMinutes(1),
        });
        Assert.Equal(InboxClaimOutcome.Claimed, claim.Outcome);
    }

    [Fact]
    public async Task MongoInboxStore_InitializeAsync_RepeatStart_ShouldReportReady()
    {
        var store = new MongoInboxStore(NewDatabase());

        await store.InitializeAsync();
        var second = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Ready, second.Outcome);
        Assert.Equal(1, second.PersistedSchemaVersion);
    }

    [Fact]
    public async Task MongoInboxStore_InitializeAsync_ConcurrentStart_ShouldNeverRaceDestructively()
    {
        var store = new MongoInboxStore(NewDatabase());

        var attempts = Enumerable.Range(0, 8).Select(_ => store.InitializeAsync());
        var results = await Task.WhenAll(attempts);

        Assert.All(results, r => Assert.True(r.IsReady, $"{r.Outcome}: {r.Message}"));

        var claim = await store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "m1", ChannelKey = Guid.NewGuid(), FeederId = Guid.NewGuid(), SchemaVersion = 1,
            PayloadContentType = "application/json", Payload = [1], LeaseOwner = "w1", LeaseDuration = TimeSpan.FromMinutes(1),
        });
        Assert.Equal(InboxClaimOutcome.Claimed, claim.Outcome);
    }

    [Fact]
    public async Task MongoInboxStore_InitializeAsync_UpgradesAnOlderPersistedVersion()
    {
        var database = NewDatabase();
        var store = new MongoInboxStore(database);
        await store.InitializeAsync();

        // Simulate a database whose schema was created by an older version of this code.
        await database.GetCollection<BsonDocument>(MongoInboxStore.DefaultMetaCollectionName)
            .UpdateOneAsync(Builders<BsonDocument>.Filter.Eq("_id", "schema"), Builders<BsonDocument>.Update.Set("Version", 0));

        var result = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Upgraded, result.Outcome);
        Assert.Equal(0, result.PersistedSchemaVersion);
        Assert.Equal(1, result.RequiredSchemaVersion);
    }

    [Fact]
    public async Task MongoInboxStore_InitializeAsync_VerifyOnly_NeverCreatesSchema()
    {
        var database = NewDatabase();
        var store = new MongoInboxStore(database);

        var result = await store.InitializeAsync(InboxMode.VerifyOnly);

        Assert.Equal(InboxOutcome.IncompatibleVersion, result.Outcome);
        Assert.False(result.IsReady);

        var collections = await (await database.ListCollectionNamesAsync()).ToListAsync();
        Assert.DoesNotContain(MongoInboxStore.DefaultMetaCollectionName, collections);
    }

    [Fact]
    public async Task MongoInboxStore_InitializeAsync_VerifyOnly_ReportsCompatibleAfterApply()
    {
        var store = new MongoInboxStore(NewDatabase());
        await store.InitializeAsync();

        var result = await store.InitializeAsync(InboxMode.VerifyOnly);

        Assert.Equal(InboxOutcome.VerifiedCompatible, result.Outcome);
        Assert.True(result.IsReady);
    }

    [Fact]
    public async Task MongoOutboxStore_InitializeAsync_FirstStart_ShouldCreateSchemaAtRequiredVersion()
    {
        var store = new MongoOutboxStore(NewDatabase());

        var result = await store.InitializeAsync();

        Assert.Equal(OutboxOutcome.Upgraded, result.Outcome);
        Assert.True(result.IsReady);

        var enqueued = await store.EnqueueAsync(new OutboxEnqueueRequest
        {
            MessageId = "m1", ProviderKey = "provider-a", SchemaVersion = 1, PayloadContentType = "application/json", Payload = [1],
        });
        Assert.NotEqual(Guid.Empty, enqueued.Id);
    }

    [Fact]
    public async Task MongoOutboxStore_InitializeAsync_ConcurrentStart_ShouldNeverRaceDestructively()
    {
        var store = new MongoOutboxStore(NewDatabase());

        var attempts = Enumerable.Range(0, 8).Select(_ => store.InitializeAsync());
        var results = await Task.WhenAll(attempts);

        Assert.All(results, r => Assert.True(r.IsReady, $"{r.Outcome}: {r.Message}"));

        var enqueued = await store.EnqueueAsync(new OutboxEnqueueRequest
        {
            MessageId = "m1", ProviderKey = "provider-a", SchemaVersion = 1, PayloadContentType = "application/json", Payload = [1],
        });
        Assert.NotEqual(Guid.Empty, enqueued.Id);
    }

    [Fact]
    public async Task MongoOutboxStore_InitializeAsync_UpgradesAnOlderPersistedVersion()
    {
        var database = NewDatabase();
        var store = new MongoOutboxStore(database);
        await store.InitializeAsync();

        await database.GetCollection<BsonDocument>(MongoOutboxStore.DefaultMetaCollectionName)
            .UpdateOneAsync(Builders<BsonDocument>.Filter.Eq("_id", "schema"), Builders<BsonDocument>.Update.Set("Version", 0));

        var result = await store.InitializeAsync();

        Assert.Equal(OutboxOutcome.Upgraded, result.Outcome);
        Assert.Equal(0, result.PersistedSchemaVersion);
    }

    [Fact]
    public async Task MongoOutboxStore_InitializeAsync_VerifyOnly_NeverCreatesSchema()
    {
        var database = NewDatabase();
        var store = new MongoOutboxStore(database);

        var result = await store.InitializeAsync(OutboxMode.VerifyOnly);

        Assert.Equal(OutboxOutcome.IncompatibleVersion, result.Outcome);
        Assert.False(result.IsReady);
    }
}
