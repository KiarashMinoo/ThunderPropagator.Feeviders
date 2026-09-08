using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxStoreFactoryTests
    {
        [Fact]
        public void GetStore_RegisteredName_ShouldReturnTheCreatedInstance()
        {
            var store = new ReferenceInboxStore(TimeProvider.System);
            var provider = BuildProvider(services => services.AddInboxStore("orders", InboxStoreType.InMemory, _ => store));

            var resolved = provider.GetRequiredService<IInboxStoreFactory>().GetStore("orders", InboxStoreType.InMemory);

            Assert.Same(store, resolved);
        }

        [Fact]
        public void GetStore_SameNameFromTwoCallers_ShouldReturnTheSameSharedInstance()
        {
            // Simulates two Feeviders configured with the same StoreConnectionName sharing one backend.
            var provider = BuildProvider(services => services.AddInboxStore("shared", InboxStoreType.Redis, _ => new ReferenceInboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IInboxStoreFactory>();

            var feederA = factory.GetStore("shared", InboxStoreType.Redis);
            var feederB = factory.GetStore("shared", InboxStoreType.Redis);

            Assert.Same(feederA, feederB);
        }

        [Fact]
        public void GetStore_DifferentNames_ShouldReturnIsolatedInstances()
        {
            var provider = BuildProvider(services => services
                .AddInboxStore("orders", InboxStoreType.Redis, _ => new ReferenceInboxStore(TimeProvider.System))
                .AddInboxStore("payments", InboxStoreType.EFCore, _ => new ReferenceInboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IInboxStoreFactory>();

            var orders = factory.GetStore("orders", InboxStoreType.Redis);
            var payments = factory.GetStore("payments", InboxStoreType.EFCore);

            Assert.NotSame(orders, payments);
        }

        [Fact]
        public void GetStore_CreateStoreDelegate_ShouldRunAtMostOnceAcrossConcurrentFirstAccess()
        {
            var invocationCount = 0;
            var provider = BuildProvider(services => services.AddInboxStore("orders", InboxStoreType.InMemory, _ =>
            {
                Interlocked.Increment(ref invocationCount);
                return new ReferenceInboxStore(TimeProvider.System);
            }));
            var factory = provider.GetRequiredService<IInboxStoreFactory>();

            Parallel.For(0, 16, _ => factory.GetStore("orders", InboxStoreType.InMemory));

            Assert.Equal(1, invocationCount);
        }

        [Fact]
        public void GetStore_UnregisteredName_ShouldThrow()
        {
            var provider = BuildProvider(services => services.AddInboxStore("orders", InboxStoreType.InMemory, _ => new ReferenceInboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IInboxStoreFactory>();

            var exception = Assert.Throws<InvalidOperationException>(() => factory.GetStore("payments", InboxStoreType.InMemory));

            Assert.Contains("payments", exception.Message);
        }

        [Fact]
        public void GetStore_RegisteredUnderADifferentStoreType_ShouldThrow()
        {
            var provider = BuildProvider(services => services.AddInboxStore("orders", InboxStoreType.Redis, _ => new ReferenceInboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IInboxStoreFactory>();

            var exception = Assert.Throws<InvalidOperationException>(() => factory.GetStore("orders", InboxStoreType.EFCore));

            Assert.Contains("Redis", exception.Message);
            Assert.Contains("EFCore", exception.Message);
        }

        [Fact]
        public void AddInboxStore_DuplicateStoreName_ShouldThrowImmediately()
        {
            var services = new ServiceCollection();
            services.AddInboxStore("orders", InboxStoreType.InMemory, _ => new ReferenceInboxStore(TimeProvider.System));

            var exception = Assert.Throws<InvalidOperationException>(() =>
                services.AddInboxStore("orders", InboxStoreType.Redis, _ => new ReferenceInboxStore(TimeProvider.System)));

            Assert.Contains("orders", exception.Message);
        }

        [Fact]
        public void AddInboxStore_EmptyStoreName_ShouldThrow()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentException>(() => services.AddInboxStore(" ", InboxStoreType.InMemory, _ => new ReferenceInboxStore(TimeProvider.System)));
        }

        [Fact]
        public void Dispose_ShouldDisposeOnlyStoresThatWereActuallyResolved()
        {
            var resolved = new DisposableTestStore();
            var neverResolved = new DisposableTestStore();
            var provider = BuildProvider(services => services
                .AddInboxStore("resolved", InboxStoreType.InMemory, _ => resolved)
                .AddInboxStore("unused", InboxStoreType.InMemory, _ => neverResolved));
            var factory = (InboxStoreFactory)provider.GetRequiredService<IInboxStoreFactory>();
            factory.GetStore("resolved", InboxStoreType.InMemory);

            factory.Dispose();

            Assert.True(resolved.Disposed);
            Assert.False(neverResolved.Disposed);
        }

        [Fact]
        public async Task DisposeAsync_ShouldPreferAsyncDisposalWhenAvailable()
        {
            var store = new DisposableTestStore();
            var provider = BuildProvider(services => services.AddInboxStore("orders", InboxStoreType.InMemory, _ => store));
            var factory = (InboxStoreFactory)provider.GetRequiredService<IInboxStoreFactory>();
            factory.GetStore("orders", InboxStoreType.InMemory);

            await factory.DisposeAsync();

            Assert.True(store.AsyncDisposed);
            Assert.False(store.Disposed);
        }

        [Fact]
        public void GetStore_AfterDispose_ShouldThrowObjectDisposedException()
        {
            var provider = BuildProvider(services => services.AddInboxStore("orders", InboxStoreType.InMemory, _ => new ReferenceInboxStore(TimeProvider.System)));
            var factory = (InboxStoreFactory)provider.GetRequiredService<IInboxStoreFactory>();
            factory.Dispose();

            Assert.Throws<ObjectDisposedException>(() => factory.GetStore("orders", InboxStoreType.InMemory));
        }

        private static ServiceProvider BuildProvider(Action<IServiceCollection> configure)
        {
            var services = new ServiceCollection();
            configure(services);
            return services.BuildServiceProvider();
        }

        private sealed class DisposableTestStore : IInboxStore, IDisposable, IAsyncDisposable
        {
            public bool Disposed { get; private set; }
            public bool AsyncDisposed { get; private set; }

            public void Dispose() => Disposed = true;

            public ValueTask DisposeAsync()
            {
                AsyncDisposed = true;
                return ValueTask.CompletedTask;
            }

            public Task<InboxClaimResult> TryClaimAsync(InboxClaimRequest request, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<InboxMessage?> GetAsync(string messageId, Guid channelKey, string? partitionKey, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<InboxMessage?> CompleteAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<InboxMessage?> FailAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<InboxMessage?> DeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<InboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<IReadOnlyList<InboxMessage>> QueryRetryableAsync(Guid channelKey, int maxCount, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<int> PurgeAsync(Guid channelKey, DateTimeOffset olderThanUtc, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<InboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();
        }
    }
}
