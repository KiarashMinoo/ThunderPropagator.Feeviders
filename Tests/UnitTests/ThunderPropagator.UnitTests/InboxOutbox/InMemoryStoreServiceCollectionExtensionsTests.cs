using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.InMemory;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.InMemory;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Covers issue #123's DI-extension and isolation acceptance criteria for the InMemory store
    /// packages - the store correctness itself is covered by <see cref="InMemoryInboxStoreContractTests"/>/
    /// <see cref="InMemoryOutboxStoreContractTests"/>.
    /// </summary>
    public class InMemoryStoreServiceCollectionExtensionsTests
    {
        [Fact]
        public void AddInMemoryInboxStore_ShouldRegisterAResolvableInMemoryInboxStore()
        {
            var services = new ServiceCollection();
            services.AddInMemoryInboxStore("orders");
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IInboxStoreFactory>().GetStore("orders", InboxStoreType.InMemory);

            Assert.IsType<InMemoryInboxStore>(store);
        }

        [Fact]
        public void AddInMemoryOutboxStore_ShouldRegisterAResolvableInMemoryOutboxStore()
        {
            var services = new ServiceCollection();
            services.AddInMemoryOutboxStore("orders");
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.InMemory);

            Assert.IsType<InMemoryOutboxStore>(store);
        }

        [Fact]
        public async Task AddInMemoryInboxStore_DifferentNames_ShouldBeIsolatedFromEachOther()
        {
            var services = new ServiceCollection();
            services.AddInMemoryInboxStore("tenant-a");
            services.AddInMemoryInboxStore("tenant-b");
            using var provider = services.BuildServiceProvider();
            var factory = provider.GetRequiredService<IInboxStoreFactory>();
            var channelKey = Guid.NewGuid();

            await factory.GetStore("tenant-a", InboxStoreType.InMemory).TryClaimAsync(new InboxClaimRequest
            {
                MessageId = "m1",
                ChannelKey = channelKey,
                FeederId = Guid.NewGuid(),
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1],
                LeaseOwner = "owner",
                LeaseDuration = TimeSpan.FromMinutes(1),
            });

            var seenByTenantB = await factory.GetStore("tenant-b", InboxStoreType.InMemory).GetAsync("m1", channelKey, null);

            Assert.Null(seenByTenantB); // tenant-a's claim never leaked into tenant-b's isolated store
        }

        [Fact]
        public async Task AddInMemoryOutboxStore_DifferentNames_ShouldBeIsolatedFromEachOther()
        {
            var services = new ServiceCollection();
            services.AddInMemoryOutboxStore("tenant-a");
            services.AddInMemoryOutboxStore("tenant-b");
            using var provider = services.BuildServiceProvider();
            var factory = provider.GetRequiredService<IOutboxStoreFactory>();

            await factory.GetStore("tenant-a", OutboxStoreType.InMemory).EnqueueAsync(new OutboxEnqueueRequest
            {
                MessageId = "m1",
                ProviderKey = "provider-a",
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1],
            });

            Assert.Equal(0, await factory.GetStore("tenant-b", OutboxStoreType.InMemory).GetDepthAsync(null));
        }

        [Fact]
        public async Task AddInMemoryOutboxStore_ShouldUseTheSuppliedTimeProviderForDeterministicTimestamps()
        {
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var services = new ServiceCollection();
            services.AddInMemoryOutboxStore("orders", timeProvider);
            using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.InMemory);

            var enqueued = await store.EnqueueAsync(new OutboxEnqueueRequest
            {
                MessageId = "m1",
                ProviderKey = "provider-a",
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1],
            });

            Assert.Equal(DateTimeOffset.UnixEpoch, enqueued.CreatedAtUtc);
        }
    }
}
