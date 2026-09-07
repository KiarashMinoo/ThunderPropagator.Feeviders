using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.Redis;
using ThunderPropagator.UnitTests.InboxOutbox;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Runs the shared <see cref="InboxStoreContractTests"/> suite (issue #109's acceptance criteria)
/// against a real <see cref="RedisInboxStore"/> talking to an actual Redis container - the same
/// suite every other <see cref="IInboxStore"/> backend (Reference, InMemory) is held to (issue #124's
/// "backend contract suites pass against real Redis" acceptance criterion).
/// </summary>
[Trait("Category", "Integration")]
public sealed class RedisInboxStoreContractTests(RedisContainerFixture fixture) : InboxStoreContractTests, IClassFixture<RedisContainerFixture>
{
    protected override IInboxStore CreateStore(TimeProvider timeProvider) =>
        new RedisInboxStore(fixture.Multiplexer, keyPrefix: Guid.NewGuid().ToString("N"), timeProvider);
}
