using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.UnitTests.InboxOutbox;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Runs the shared <see cref="InboxStoreContractTests"/> suite (issue #109's acceptance criteria)
/// against a real <see cref="EfCoreInboxStore"/> talking to an actual PostgreSQL container - the same
/// suite every other <see cref="IInboxStore"/> backend is held to (issue #125's "store contract suites
/// pass on supported providers" acceptance criterion, PostgreSQL tier - see
/// <see cref="PostgresContainerFixture"/>'s remarks for SQL Server/MySQL).
/// </summary>
[Trait("Category", "Integration")]
public sealed class EfCoreInboxStoreContractTests(PostgresContainerFixture fixture) : InboxStoreContractTests, IClassFixture<PostgresContainerFixture>
{
    protected override IInboxStore CreateStore(TimeProvider timeProvider)
    {
        var schema = $"s{Guid.NewGuid():N}";
        var options = EfCoreTestDbContext.BuildOptions(fixture.GetConnectionString());

        using (var db = new EfCoreTestDbContext(options, schema))
            db.CreateTables();

        return new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema), timeProvider);
    }
}
