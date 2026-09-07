using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Covers the DI-extension half of issue #125 ("provide DI and provider capability abstraction")
    /// without a real database - unlike the Redis backend, <see cref="EfCoreInboxStore"/>/
    /// <see cref="EfCoreOutboxStore"/> never resolve their <see cref="DbContext"/> factory eagerly (a new
    /// context is only created per operation), so <c>GetStore</c> alone never invokes the supplied
    /// factory delegate. Actual behavior against a real database is covered by the EF Core integration
    /// test project's own contract-suite, concurrency, and health-check tests (issue #125's "against real
    /// providers" requirement).
    /// </summary>
    public class EfCoreStoreServiceCollectionExtensionsTests
    {
        [Fact]
        public void AddEfCoreInboxStore_ShouldRegisterAResolvableEfCoreInboxStore()
        {
            var services = new ServiceCollection();
            services.AddEfCoreInboxStore("orders", _ => throw new NotImplementedException("never invoked by GetStore"));
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IInboxStoreFactory>().GetStore("orders", InboxStoreType.EFCore);

            Assert.IsType<EfCoreInboxStore>(store);
        }

        [Fact]
        public void AddEfCoreOutboxStore_ShouldRegisterAResolvableEfCoreOutboxStore()
        {
            var services = new ServiceCollection();
            services.AddEfCoreOutboxStore("orders", _ => throw new NotImplementedException("never invoked by GetStore"));
            using var provider = services.BuildServiceProvider();

            var store = provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.EFCore);

            Assert.IsType<EfCoreOutboxStore>(store);
        }
    }
}
