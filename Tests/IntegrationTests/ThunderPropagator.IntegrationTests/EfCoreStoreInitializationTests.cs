using Microsoft.EntityFrameworkCore;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Providers.DotNet.Outbox;
using Xunit;
using InboxMode = ThunderPropagator.Feeders.Inbox.StoreInitializationMode;
using InboxOutcome = ThunderPropagator.Feeders.Inbox.StoreInitializationOutcome;
using OutboxMode = ThunderPropagator.Providers.DotNet.Outbox.StoreInitializationMode;
using OutboxOutcome = ThunderPropagator.Providers.DotNet.Outbox.StoreInitializationOutcome;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Covers issue #127's "idempotent under concurrent replicas", "workers remain unready until required
/// schema is valid", and "external migration mode verifies schema without mutating it" acceptance
/// criteria for <see cref="EfCoreInboxStore.InitializeAsync"/>/<see cref="EfCoreOutboxStore.InitializeAsync"/>,
/// against a real PostgreSQL container - see <see cref="PostgresContainerFixture"/>'s remarks for why
/// SQL Server/MySQL are implemented but not exercised here.
/// </summary>
[Trait("Category", "Integration")]
public sealed class EfCoreStoreInitializationTests(PostgresContainerFixture fixture) : IClassFixture<PostgresContainerFixture>
{
    private (string Schema, DbContextOptions<EfCoreTestDbContext> Options) NewUninitializedSchema() =>
        ($"s{Guid.NewGuid():N}", EfCoreTestDbContext.BuildOptions(fixture.GetConnectionString()));

    [Fact]
    public async Task EfCoreInboxStore_InitializeAsync_FirstStart_ShouldCreateTablesAtRequiredVersion()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));

        var result = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Upgraded, result.Outcome);
        Assert.True(result.IsReady);
        Assert.Null(result.PersistedSchemaVersion);

        // Tables now exist and the store is usable.
        var claim = await store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "m1", ChannelKey = Guid.NewGuid(), FeederId = Guid.NewGuid(), SchemaVersion = 1,
            PayloadContentType = "application/json", Payload = [1], LeaseOwner = "w1", LeaseDuration = TimeSpan.FromMinutes(1),
        });
        Assert.Equal(InboxClaimOutcome.Claimed, claim.Outcome);
    }

    [Fact]
    public async Task EfCoreInboxStore_InitializeAsync_RepeatStart_ShouldReportReadyWithoutReapplying()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));

        await store.InitializeAsync();
        var second = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Ready, second.Outcome);
        Assert.Equal(1, second.PersistedSchemaVersion);
    }

    [Fact]
    public async Task EfCoreInboxStore_InitializeAsync_ConcurrentStart_ShouldNeverRaceDestructively()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));

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
    public async Task EfCoreInboxStore_InitializeAsync_UpgradesAnOlderPersistedVersion()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));
        await store.InitializeAsync();

        // Simulate a database whose schema was created by an older version of this code.
        await using (var db = new EfCoreTestDbContext(options, schema))
        {
            var row = await db.Set<InboxSchemaVersion>().SingleAsync();
            row.Version = 0;
            await db.SaveChangesAsync();
        }

        var result = await store.InitializeAsync();

        Assert.Equal(InboxOutcome.Upgraded, result.Outcome);
        Assert.Equal(0, result.PersistedSchemaVersion);
        Assert.Equal(1, result.RequiredSchemaVersion);
    }

    [Fact]
    public async Task EfCoreInboxStore_InitializeAsync_VerifyOnly_NeverCreatesTables()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));

        var result = await store.InitializeAsync(InboxMode.VerifyOnly);

        Assert.Equal(InboxOutcome.IncompatibleVersion, result.Outcome);
        Assert.False(result.IsReady);

        await Assert.ThrowsAnyAsync<Exception>(() => store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "m1", ChannelKey = Guid.NewGuid(), FeederId = Guid.NewGuid(), SchemaVersion = 1,
            PayloadContentType = "application/json", Payload = [1], LeaseOwner = "w1", LeaseDuration = TimeSpan.FromMinutes(1),
        }));
    }

    [Fact]
    public async Task EfCoreInboxStore_InitializeAsync_VerifyOnly_ReportsCompatibleAfterApply()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));
        await store.InitializeAsync();

        var result = await store.InitializeAsync(InboxMode.VerifyOnly);

        Assert.Equal(InboxOutcome.VerifiedCompatible, result.Outcome);
        Assert.True(result.IsReady);
    }

    [Fact]
    public async Task EfCoreOutboxStore_InitializeAsync_FirstStart_ShouldCreateTablesAtRequiredVersion()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreOutboxStore(() => new EfCoreTestDbContext(options, schema));

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
    public async Task EfCoreOutboxStore_InitializeAsync_ConcurrentStart_ShouldNeverRaceDestructively()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreOutboxStore(() => new EfCoreTestDbContext(options, schema));

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
    public async Task EfCoreOutboxStore_InitializeAsync_UpgradesAnOlderPersistedVersion()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreOutboxStore(() => new EfCoreTestDbContext(options, schema));
        await store.InitializeAsync();

        await using (var db = new EfCoreTestDbContext(options, schema))
        {
            var row = await db.Set<OutboxSchemaVersion>().SingleAsync();
            row.Version = 0;
            await db.SaveChangesAsync();
        }

        var result = await store.InitializeAsync();

        Assert.Equal(OutboxOutcome.Upgraded, result.Outcome);
        Assert.Equal(0, result.PersistedSchemaVersion);
    }

    [Fact]
    public async Task EfCoreOutboxStore_InitializeAsync_VerifyOnly_NeverCreatesTables()
    {
        var (schema, options) = NewUninitializedSchema();
        var store = new EfCoreOutboxStore(() => new EfCoreTestDbContext(options, schema));

        var result = await store.InitializeAsync(OutboxMode.VerifyOnly);

        Assert.Equal(OutboxOutcome.IncompatibleVersion, result.Outcome);
        Assert.False(result.IsReady);
    }
}
