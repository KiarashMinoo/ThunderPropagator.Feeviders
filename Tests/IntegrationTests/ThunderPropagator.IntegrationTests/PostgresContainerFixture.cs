using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// One real PostgreSQL container shared across every test in a class via <see cref="IClassFixture{TFixture}"/> -
/// starting a fresh container per test would make the suite prohibitively slow. Isolation between tests
/// instead comes from each test using its own unique schema name (see <c>EfCoreInboxStoreContractTests</c>/
/// <c>EfCoreOutboxStoreContractTests</c>), not from a fresh server.
/// </summary>
/// <remarks>
/// Represents this repo's PostgreSQL CI tier for issue #125's EF Core backend - SQL Server and MySQL use
/// the exact same <c>EfCoreInboxStore</c>/<c>EfCoreOutboxStore</c> code path (selecting their own
/// <c>IOutboxClaimSqlDialect</c> at runtime from the active provider), but are not separately
/// containerized and tested in this environment; see the #125 commit message for the documented gap.
/// </remarks>
public sealed class PostgresContainerFixture : IAsyncLifetime
{
    private const int PostgresPort = 5432;
    private const string Password = "postgres";
    private readonly IContainer _container = new ContainerBuilder("postgres:16-alpine")
        .WithPortBinding(PostgresPort, true)
        .WithEnvironment("POSTGRES_PASSWORD", Password)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("database system is ready to accept connections"))
        .Build();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    public string GetConnectionString(string database = "postgres") =>
        $"Host={_container.Hostname};Port={_container.GetMappedPublicPort(PostgresPort)};Database={database};Username=postgres;Password={Password}";
}
