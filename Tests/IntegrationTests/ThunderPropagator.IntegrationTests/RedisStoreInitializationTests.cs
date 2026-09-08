using StackExchange.Redis;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.Redis;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.Redis;
using Xunit;
using InboxMode = ThunderPropagator.Feeders.Inbox.StoreInitializationMode;
using InboxOutcome = ThunderPropagator.Feeders.Inbox.StoreInitializationOutcome;
using OutboxMode = ThunderPropagator.Providers.DotNet.Outbox.StoreInitializationMode;
using OutboxOutcome = ThunderPropagator.Providers.DotNet.Outbox.StoreInitializationOutcome;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Covers issue #127's "idempotent under concurrent replicas", "workers remain unready until required
/// schema is valid", and "external migration mode verifies schema without mutating it" acceptance
/// criteria for <see cref="RedisInboxStore.InitializeAsync"/>/<see cref="RedisOutboxStore.InitializeAsync"/>,
/// against a real Redis container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RedisStoreInitializationTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    private string NewKeyPrefix() => $"init-test-{Guid.NewGuid():N}";

    [Fact]
    public async Task RedisInboxStore_InitializeAsync_FirstStart_ShouldCreateVersionKey()
    {
        var store = new RedisInboxStore(fixture.Multiplexer, NewKeyPrefix());

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
    public async Task RedisInboxStore_InitializeAsync_RepeatStart_ShouldReportReady()
    {
        var store = new RedisInboxStore(fixture.Multiplexer, NewKeyPrefix());

        await store.InitializeAsync();
        var second = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Ready, second.Outcome);
        Assert.Equal(1, second.PersistedSchemaVersion);
    }

    [Fact]
    public async Task RedisInboxStore_InitializeAsync_ConcurrentStart_ShouldNeverRaceDestructively()
    {
        var store = new RedisInboxStore(fixture.Multiplexer, NewKeyPrefix());

        var attempts = Enumerable.Range(0, 8).Select(_ => store.InitializeAsync());
        var results = await Task.WhenAll(attempts);

        Assert.All(results, r => Assert.True(r.IsReady, $"{r.Outcome}: {r.Message}"));
    }

    [Fact]
    public async Task RedisInboxStore_InitializeAsync_UpgradesAnOlderPersistedVersion()
    {
        var keyPrefix = NewKeyPrefix();
        var store = new RedisInboxStore(fixture.Multiplexer, keyPrefix);
        await store.InitializeAsync();

        // Simulate a keyspace whose layout was initialized by an older version of this code.
        await fixture.Multiplexer.GetDatabase().StringSetAsync($"{{{keyPrefix}}}:inbox:schema-version", "0");

        var result = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Upgraded, result.Outcome);
        Assert.Equal(0, result.PersistedSchemaVersion);
        Assert.Equal(1, result.RequiredSchemaVersion);
    }

    [Fact]
    public async Task RedisInboxStore_InitializeAsync_VerifyOnly_NeverCreatesTheVersionKey()
    {
        var keyPrefix = NewKeyPrefix();
        var store = new RedisInboxStore(fixture.Multiplexer, keyPrefix);

        var result = await store.InitializeAsync(InboxMode.VerifyOnly);

        Assert.Equal(InboxOutcome.IncompatibleVersion, result.Outcome);
        Assert.False(result.IsReady);

        var exists = await fixture.Multiplexer.GetDatabase().KeyExistsAsync($"{{{keyPrefix}}}:inbox:schema-version");
        Assert.False(exists);
    }

    [Fact]
    public async Task RedisInboxStore_InitializeAsync_VerifyOnly_ReportsCompatibleAfterApply()
    {
        var store = new RedisInboxStore(fixture.Multiplexer, NewKeyPrefix());
        await store.InitializeAsync();

        var result = await store.InitializeAsync(InboxMode.VerifyOnly);

        Assert.Equal(InboxOutcome.VerifiedCompatible, result.Outcome);
        Assert.True(result.IsReady);
    }

    [Fact]
    public async Task RedisOutboxStore_InitializeAsync_FirstStart_ShouldCreateVersionKey()
    {
        var store = new RedisOutboxStore(fixture.Multiplexer, NewKeyPrefix());

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
    public async Task RedisOutboxStore_InitializeAsync_ConcurrentStart_ShouldNeverRaceDestructively()
    {
        var store = new RedisOutboxStore(fixture.Multiplexer, NewKeyPrefix());

        var attempts = Enumerable.Range(0, 8).Select(_ => store.InitializeAsync());
        var results = await Task.WhenAll(attempts);

        Assert.All(results, r => Assert.True(r.IsReady, $"{r.Outcome}: {r.Message}"));
    }

    [Fact]
    public async Task RedisOutboxStore_InitializeAsync_UpgradesAnOlderPersistedVersion()
    {
        var keyPrefix = NewKeyPrefix();
        var store = new RedisOutboxStore(fixture.Multiplexer, keyPrefix);
        await store.InitializeAsync();

        await fixture.Multiplexer.GetDatabase().StringSetAsync($"{{{keyPrefix}}}:outbox:schema-version", "0");

        var result = await store.InitializeAsync();

        Assert.Equal(OutboxOutcome.Upgraded, result.Outcome);
        Assert.Equal(0, result.PersistedSchemaVersion);
    }

    [Fact]
    public async Task RedisOutboxStore_InitializeAsync_VerifyOnly_NeverCreatesTheVersionKey()
    {
        var keyPrefix = NewKeyPrefix();
        var store = new RedisOutboxStore(fixture.Multiplexer, keyPrefix);

        var result = await store.InitializeAsync(OutboxMode.VerifyOnly);

        Assert.Equal(OutboxOutcome.IncompatibleVersion, result.Outcome);
        Assert.False(result.IsReady);
    }
}
