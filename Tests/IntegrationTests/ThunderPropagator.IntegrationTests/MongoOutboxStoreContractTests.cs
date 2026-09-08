using MongoDB.Driver;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.MongoDB;
using ThunderPropagator.UnitTests.InboxOutbox;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Runs the shared <see cref="OutboxStoreContractTests"/> suite (issue #116's acceptance criteria)
/// against a real <see cref="MongoOutboxStore"/> talking to an actual MongoDB replica-set container - the
/// same suite every other <see cref="IOutboxStore"/> backend is held to (issue #126's "backend contract
/// suites pass against a real replica-set test container" acceptance criterion).
/// </summary>
[Trait("Category", "Integration")]
public sealed class MongoOutboxStoreContractTests(MongoContainerFixture fixture) : OutboxStoreContractTests, IClassFixture<MongoContainerFixture>
{
    protected override IOutboxStore CreateStore(TimeProvider timeProvider)
    {
        var client = new MongoClient(fixture.ConnectionString);
        var database = client.GetDatabase($"db{Guid.NewGuid():N}");
        var store = new MongoOutboxStore(database, timeProvider: timeProvider);
        store.EnsureIndexesAsync().GetAwaiter().GetResult();
        return store;
    }
}
