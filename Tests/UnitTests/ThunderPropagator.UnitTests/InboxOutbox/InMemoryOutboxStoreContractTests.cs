using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.InMemory;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>Runs the shared <see cref="OutboxStoreContractTests"/> suite against the real <see cref="InMemoryOutboxStore"/> package (issue #123).</summary>
    public sealed class InMemoryOutboxStoreContractTests : OutboxStoreContractTests
    {
        protected override IOutboxStore CreateStore(TimeProvider timeProvider) => new InMemoryOutboxStore(timeProvider);
    }
}
