using MongoDB.Driver;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.MongoDB;
using ThunderPropagator.UnitTests.InboxOutbox;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Runs the shared <see cref="InboxStoreContractTests"/> suite (issue #109's acceptance criteria)
/// against a real <see cref="MongoInboxStore"/> talking to an actual MongoDB replica-set container - the
/// same suite every other <see cref="IInboxStore"/> backend is held to (issue #126's "backend contract
/// suites pass against a real replica-set test container" acceptance criterion).
/// </summary>
[Trait("Category", "Integration")]
public sealed class MongoInboxStoreContractTests(MongoContainerFixture fixture) : InboxStoreContractTests, IClassFixture<MongoContainerFixture>
{
    protected override IInboxStore CreateStore(TimeProvider timeProvider)
    {
        var client = new MongoClient(fixture.ConnectionString);
        var database = client.GetDatabase($"db{Guid.NewGuid():N}");
        var store = new MongoInboxStore(database, timeProvider: timeProvider);
        store.EnsureIndexesAsync().GetAwaiter().GetResult();
        return store;
    }
}
