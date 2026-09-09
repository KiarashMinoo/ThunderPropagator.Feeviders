using Testcontainers.RabbitMq;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// One real RabbitMQ broker shared across every test in a class via <see cref="IClassFixture{TFixture}"/>.
/// Unlike the other broker/store fixtures in this project, tests using this fixture deliberately stop
/// and restart the SAME container mid-test (see <see cref="StopBrokerAsync"/>/<see cref="StartBrokerAsync"/>)
/// to simulate an outage and its recovery - a fresh container per test would not let a test observe "the
/// broker that was down is now back," only "a different, healthy broker."
/// </summary>
public sealed class RabbitMqContainerFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:3.13-management-alpine").Build();

    // Re-resolved after every (re)start - GetMappedPublicPort throws while the container is stopped,
    // and a restart of this same container is not guaranteed to keep the exact same host port, so
    // callers must read this only once the broker is confirmed running (i.e. after StartBrokerAsync
    // returns), never while it is known to be down.
    public string ConnectionString { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    /// <summary>Stops the broker container - simulates an outage.</summary>
    public Task StopBrokerAsync(CancellationToken cancellationToken = default) => _container.StopAsync(cancellationToken);

    /// <summary>Restarts the same, previously-stopped broker container and refreshes <see cref="ConnectionString"/> - simulates recovery.</summary>
    public async Task StartBrokerAsync(CancellationToken cancellationToken = default)
    {
        await _container.StartAsync(cancellationToken);
        ConnectionString = _container.GetConnectionString();
    }
}
