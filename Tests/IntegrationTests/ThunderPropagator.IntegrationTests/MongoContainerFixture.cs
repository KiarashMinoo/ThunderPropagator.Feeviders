using Testcontainers.MongoDb;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// One real MongoDB container shared across every test in a class via <see cref="IClassFixture{TFixture}"/>.
/// Uses the dedicated <c>Testcontainers.MongoDb</c> module rather than a generic <c>ContainerBuilder</c>
/// (contrast <see cref="PostgresContainerFixture"/>/<c>RedisContainerFixture</c>): unlike Postgres/Redis,
/// starting MongoDB is not enough on its own - issue #126's "against a real replica-set test container
/// where transactions are tested" requires an actual replica set (a standalone <c>mongod</c> cannot run
/// multi-document transactions at all), which needs <c>--replSet</c> plus an <c>rs.initiate()</c> call and
/// a wait for that member to become PRIMARY before the container is usable. The module handles all of
/// that itself; isolation between tests comes from each test creating its own client against a uniquely
/// named database from <see cref="ConnectionString"/>, not from a fresh container.
/// </summary>
public sealed class MongoContainerFixture : IAsyncLifetime
{
    private readonly MongoDbContainer _container = new MongoDbBuilder("mongo:7.0").WithReplicaSet("rs0").Build();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    public string ConnectionString => _container.GetConnectionString();
}
