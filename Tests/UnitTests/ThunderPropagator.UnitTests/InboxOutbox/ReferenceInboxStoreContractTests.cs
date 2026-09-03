using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>Runs the shared <see cref="InboxStoreContractTests"/> suite against <see cref="ReferenceInboxStore"/>.</summary>
    public sealed class ReferenceInboxStoreContractTests : InboxStoreContractTests
    {
        protected override IInboxStore CreateStore(TimeProvider timeProvider) => new ReferenceInboxStore(timeProvider);
    }
}
