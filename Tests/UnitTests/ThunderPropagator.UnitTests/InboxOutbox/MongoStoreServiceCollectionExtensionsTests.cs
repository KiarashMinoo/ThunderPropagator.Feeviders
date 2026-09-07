using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.MongoDB;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.MongoDB;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Covers the DI-extension half of issue #126's "provide DI and health integration" acceptance
    /// criterion without a real MongoDB server - neither constructing an <see cref="IMongoDatabase"/> via
    /// <see cref="MongoClient"/> nor calling <see cref="IMongoDatabase.GetCollection{TDocument}(string, MongoCollectionSettings)"/>
    /// establishes an actual connection (the driver connects lazily, on first real operation), so a
    /// client pointed at an address nothing needs to answer is enough here. Actual behavior against a
    /// real server is covered by the MongoDB integration test project's own contract-suite, concurrency,
    /// transaction, and health-check tests (issue #126's "against a real replica-set test container"
    /// requirement).
    /// </summary>
    public class MongoStoreServiceCollectionExtensionsTests
    {
        private static IMongoDatabase CreateDatabase() => new MongoClient("mongodb://localhost:1").GetDatabase("orders");

        [Fact]
        public void AddMongoInboxStore_ShouldRegisterAResolvableMongoInboxStore()
        {
            var services = new ServiceCollection();
            services.AddMongoInboxStore("orders", _ => CreateDatabase());
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IInboxStoreFactory>().GetStore("orders", InboxStoreType.MongoDB);

            Assert.IsType<MongoInboxStore>(store);
        }

        [Fact]
        public void AddMongoOutboxStore_ShouldRegisterAResolvableMongoOutboxStore()
        {
            var services = new ServiceCollection();
            services.AddMongoOutboxStore("orders", _ => CreateDatabase());
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.MongoDB);

            Assert.IsType<MongoOutboxStore>(store);
        }

        [Fact]
        public void AddMongoInboxStore_ShouldResolveTheDatabaseThroughTheSuppliedFactory()
        {
            var services = new ServiceCollection();
            var resolveCallCount = 0;
            services.AddMongoInboxStore("orders", _ =>
            {
                resolveCallCount++;
                return CreateDatabase();
            });
            using var provider = services.BuildServiceProvider();

            provider.GetRequiredService<IInboxStoreFactory>().GetStore("orders", InboxStoreType.MongoDB);

            Assert.Equal(1, resolveCallCount);
        }

        [Fact]
        public void AddMongoOutboxStore_ShouldResolveTheDatabaseThroughTheSuppliedFactory()
        {
            var services = new ServiceCollection();
            var resolveCallCount = 0;
            services.AddMongoOutboxStore("orders", _ =>
            {
                resolveCallCount++;
                return CreateDatabase();
            });
            using var provider = services.BuildServiceProvider();

            provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.MongoDB);

            Assert.Equal(1, resolveCallCount);
        }
    }
}
