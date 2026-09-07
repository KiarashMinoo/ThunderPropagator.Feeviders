using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.InMemory;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>Runs the shared <see cref="InboxStoreContractTests"/> suite against the real <see cref="InMemoryInboxStore"/> package (issue #123).</summary>
    public sealed class InMemoryInboxStoreContractTests : InboxStoreContractTests
    {
        protected override IInboxStore CreateStore(TimeProvider timeProvider) => new InMemoryInboxStore(timeProvider);
    }
}
