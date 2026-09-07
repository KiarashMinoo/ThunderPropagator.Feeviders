using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.Redis;
using ThunderPropagator.UnitTests.InboxOutbox;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Runs the shared <see cref="OutboxStoreContractTests"/> suite (issue #116's acceptance criteria)
/// against a real <see cref="RedisOutboxStore"/> talking to an actual Redis container - the same
/// suite every other <see cref="IOutboxStore"/> backend (Reference, InMemory) is held to (issue #124's
/// "backend contract suites pass against real Redis" acceptance criterion).
/// </summary>
[Trait("Category", "Integration")]
public sealed class RedisOutboxStoreContractTests(RedisContainerFixture fixture) : OutboxStoreContractTests, IClassFixture<RedisContainerFixture>
{
    protected override IOutboxStore CreateStore(TimeProvider timeProvider) =>
        new RedisOutboxStore(fixture.Multiplexer, keyPrefix: Guid.NewGuid().ToString("N"), timeProvider);
}
