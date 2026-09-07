using Microsoft.Extensions.Diagnostics.HealthChecks;
using ThunderPropagator.Feeders.Inbox.Redis;
using ThunderPropagator.Providers.DotNet.Outbox.Redis;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>Covers issue #124's "provide DI and health integration" acceptance criterion.</summary>
[Trait("Category", "Integration")]
public sealed class RedisStoreHealthCheckTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    [Fact]
    public async Task RedisInboxStore_CheckHealthAsync_ShouldReportHealthyWhileConnected()
    {
        var store = new RedisInboxStore(fixture.Multiplexer, keyPrefix: Guid.NewGuid().ToString("N"));

        var result = await store.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task RedisOutboxStore_CheckHealthAsync_ShouldReportHealthyWhileConnected()
    {
        var store = new RedisOutboxStore(fixture.Multiplexer, keyPrefix: Guid.NewGuid().ToString("N"));

        var result = await store.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }
}
