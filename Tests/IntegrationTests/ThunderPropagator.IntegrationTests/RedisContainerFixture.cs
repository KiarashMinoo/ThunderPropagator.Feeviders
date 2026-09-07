using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using StackExchange.Redis;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// One real Redis container shared across every test in a class via <see cref="IClassFixture{TFixture}"/> -
/// starting a fresh container per test would make the suite prohibitively slow. Isolation between tests
/// instead comes from each test constructing its store with its own unique key prefix (see
/// <c>RedisInboxStore</c>/<c>RedisOutboxStore</c>'s <c>keyPrefix</c> parameter), not from a fresh server.
/// </summary>
public sealed class RedisContainerFixture : IAsyncLifetime
{
    private const int RedisPort = 6379;
    private readonly IContainer _container = new ContainerBuilder("redis:7-alpine")
        .WithPortBinding(RedisPort, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(RedisPort))
        .Build();

    public IConnectionMultiplexer Multiplexer { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        Multiplexer = await ConnectionMultiplexer.ConnectAsync($"{_container.Hostname}:{_container.GetMappedPublicPort(RedisPort)}");
    }

    public async Task DisposeAsync()
    {
        await Multiplexer.DisposeAsync();
        await _container.DisposeAsync();
    }
}
