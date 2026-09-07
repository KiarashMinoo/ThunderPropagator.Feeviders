using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>Runs the shared <see cref="OutboxStoreContractTests"/> suite against <see cref="ReferenceOutboxStore"/>.</summary>
    public sealed class ReferenceOutboxStoreContractTests : OutboxStoreContractTests
    {
        protected override IOutboxStore CreateStore(TimeProvider timeProvider) => new ReferenceOutboxStore(timeProvider);
    }
}
