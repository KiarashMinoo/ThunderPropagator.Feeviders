using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using StackExchange.Redis;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.Redis;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.Redis;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Covers the DI-extension half of issue #124 ("provide DI and health integration") without a real
    /// Redis server - constructing <see cref="RedisInboxStore"/>/<see cref="RedisOutboxStore"/> never
    /// eagerly connects, so a fake <see cref="IConnectionMultiplexer"/> is enough here. Actual behavior
    /// against a real server is covered by the Redis integration test project's own contract-suite,
    /// concurrency, retention, and health-check tests (issue #124's "against real Redis" requirement).
    /// </summary>
    public class RedisStoreServiceCollectionExtensionsTests
    {
        [Fact]
        public void AddRedisInboxStore_ShouldRegisterAResolvableRedisInboxStore()
        {
            var services = new ServiceCollection();
            services.AddRedisInboxStore("orders", _ => Substitute.For<IConnectionMultiplexer>());
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IInboxStoreFactory>().GetStore("orders", InboxStoreType.Redis);

            Assert.IsType<RedisInboxStore>(store);
        }

        [Fact]
        public void AddRedisOutboxStore_ShouldRegisterAResolvableRedisOutboxStore()
        {
            var services = new ServiceCollection();
            services.AddRedisOutboxStore("orders", _ => Substitute.For<IConnectionMultiplexer>());
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.Redis);

            Assert.IsType<RedisOutboxStore>(store);
        }

        [Fact]
        public void AddRedisInboxStore_ShouldResolveTheConnectionThroughTheSuppliedFactory()
        {
            var services = new ServiceCollection();
            var multiplexer = Substitute.For<IConnectionMultiplexer>();
            var resolveCallCount = 0;
            services.AddRedisInboxStore("orders", _ =>
            {
                resolveCallCount++;
                return multiplexer;
            });
            using var provider = services.BuildServiceProvider();

            provider.GetRequiredService<IInboxStoreFactory>().GetStore("orders", InboxStoreType.Redis);

            Assert.Equal(1, resolveCallCount);
        }

        [Fact]
        public void AddRedisOutboxStore_ShouldResolveTheConnectionThroughTheSuppliedFactory()
        {
            var services = new ServiceCollection();
            var multiplexer = Substitute.For<IConnectionMultiplexer>();
            var resolveCallCount = 0;
            services.AddRedisOutboxStore("orders", _ =>
            {
                resolveCallCount++;
                return multiplexer;
            });
            using var provider = services.BuildServiceProvider();

            provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.Redis);

            Assert.Equal(1, resolveCallCount);
        }
    }
}
