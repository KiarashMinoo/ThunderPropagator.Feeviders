using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.UnitTests.InboxOutbox;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Runs the shared <see cref="OutboxStoreContractTests"/> suite (issue #116's acceptance criteria)
/// against a real <see cref="EfCoreOutboxStore"/> talking to an actual PostgreSQL container - the same
/// suite every other <see cref="IOutboxStore"/> backend is held to (issue #125's "store contract suites
/// pass on supported providers" acceptance criterion, PostgreSQL tier - see
/// <see cref="PostgresContainerFixture"/>'s remarks for SQL Server/MySQL).
/// </summary>
[Trait("Category", "Integration")]
public sealed class EfCoreOutboxStoreContractTests(PostgresContainerFixture fixture) : OutboxStoreContractTests, IClassFixture<PostgresContainerFixture>
{
    protected override IOutboxStore CreateStore(TimeProvider timeProvider)
    {
        var schema = $"s{Guid.NewGuid():N}";
        var options = EfCoreTestDbContext.BuildOptions(fixture.GetConnectionString());

        using (var db = new EfCoreTestDbContext(options, schema))
            db.CreateTables();

        return new EfCoreOutboxStore(() => new EfCoreTestDbContext(options, schema), timeProvider);
    }
}
