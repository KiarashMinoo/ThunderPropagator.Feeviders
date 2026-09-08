using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxStoreFactoryTests
    {
        [Fact]
        public void GetStore_RegisteredName_ShouldReturnTheCreatedInstance()
        {
            var store = new ReferenceOutboxStore(TimeProvider.System);
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ => store));

            var resolved = provider.GetRequiredService<IOutboxStoreFactory>().GetStore("orders", OutboxStoreType.InMemory);

            Assert.Same(store, resolved);
        }

        [Fact]
        public void GetStore_SameNameFromTwoCallers_ShouldReturnTheSameSharedInstance()
        {
            // Simulates two Providers configured with the same StoreConnectionName sharing one backend.
            var provider = BuildProvider(services => services.AddOutboxStore("shared", OutboxStoreType.Redis, _ => new ReferenceOutboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IOutboxStoreFactory>();

            var providerA = factory.GetStore("shared", OutboxStoreType.Redis);
            var providerB = factory.GetStore("shared", OutboxStoreType.Redis);

            Assert.Same(providerA, providerB);
        }

        [Fact]
        public void GetStore_DifferentNames_ShouldReturnIsolatedInstances()
        {
            var provider = BuildProvider(services => services
                .AddOutboxStore("orders", OutboxStoreType.Redis, _ => new ReferenceOutboxStore(TimeProvider.System))
                .AddOutboxStore("payments", OutboxStoreType.EFCore, _ => new ReferenceOutboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IOutboxStoreFactory>();

            var orders = factory.GetStore("orders", OutboxStoreType.Redis);
            var payments = factory.GetStore("payments", OutboxStoreType.EFCore);

            Assert.NotSame(orders, payments);
        }

        [Fact]
        public void GetStore_CreateStoreDelegate_ShouldRunAtMostOnceAcrossConcurrentFirstAccess()
        {
            var invocationCount = 0;
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ =>
            {
                Interlocked.Increment(ref invocationCount);
                return new ReferenceOutboxStore(TimeProvider.System);
            }));
            var factory = provider.GetRequiredService<IOutboxStoreFactory>();

            Parallel.For(0, 16, _ => factory.GetStore("orders", OutboxStoreType.InMemory));

            Assert.Equal(1, invocationCount);
        }

        [Fact]
        public void GetStore_UnregisteredName_ShouldThrow()
        {
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IOutboxStoreFactory>();

            var exception = Assert.Throws<InvalidOperationException>(() => factory.GetStore("payments", OutboxStoreType.InMemory));

            Assert.Contains("payments", exception.Message);
        }

        [Fact]
        public void GetStore_RegisteredUnderADifferentStoreType_ShouldThrow()
        {
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.Redis, _ => new ReferenceOutboxStore(TimeProvider.System)));
            var factory = provider.GetRequiredService<IOutboxStoreFactory>();

            var exception = Assert.Throws<InvalidOperationException>(() => factory.GetStore("orders", OutboxStoreType.EFCore));

            Assert.Contains("Redis", exception.Message);
            Assert.Contains("EFCore", exception.Message);
        }

        [Fact]
        public void AddOutboxStore_DuplicateStoreName_ShouldThrowImmediately()
        {
            var services = new ServiceCollection();
            services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System));

            var exception = Assert.Throws<InvalidOperationException>(() =>
                services.AddOutboxStore("orders", OutboxStoreType.Redis, _ => new ReferenceOutboxStore(TimeProvider.System)));

            Assert.Contains("orders", exception.Message);
        }

        [Fact]
        public void AddOutboxStore_EmptyStoreName_ShouldThrow()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentException>(() => services.AddOutboxStore(" ", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System)));
        }

        [Fact]
        public void Dispose_ShouldDisposeOnlyStoresThatWereActuallyResolved()
        {
            var resolved = new DisposableTestStore();
            var neverResolved = new DisposableTestStore();
            var provider = BuildProvider(services => services
                .AddOutboxStore("resolved", OutboxStoreType.InMemory, _ => resolved)
                .AddOutboxStore("unused", OutboxStoreType.InMemory, _ => neverResolved));
            var factory = (OutboxStoreFactory)provider.GetRequiredService<IOutboxStoreFactory>();
            factory.GetStore("resolved", OutboxStoreType.InMemory);

            factory.Dispose();

            Assert.True(resolved.Disposed);
            Assert.False(neverResolved.Disposed);
        }

        [Fact]
        public async Task DisposeAsync_ShouldPreferAsyncDisposalWhenAvailable()
        {
            var store = new DisposableTestStore();
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ => store));
            var factory = (OutboxStoreFactory)provider.GetRequiredService<IOutboxStoreFactory>();
            factory.GetStore("orders", OutboxStoreType.InMemory);

            await factory.DisposeAsync();

            Assert.True(store.AsyncDisposed);
            Assert.False(store.Disposed);
        }

        [Fact]
        public void GetStore_AfterDispose_ShouldThrowObjectDisposedException()
        {
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System)));
            var factory = (OutboxStoreFactory)provider.GetRequiredService<IOutboxStoreFactory>();
            factory.Dispose();

            Assert.Throws<ObjectDisposedException>(() => factory.GetStore("orders", OutboxStoreType.InMemory));
        }

        [Fact]
        public async Task CheckHealthAsync_ShouldReportHealthyBeforeDisposalAndListRegisteredStoreNames()
        {
            var provider = BuildProvider(services => services
                .AddOutboxStore("orders", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System))
                .AddOutboxStore("payments", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System)));
            var factory = (OutboxStoreFactory)provider.GetRequiredService<IOutboxStoreFactory>();

            var result = await factory.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Contains("orders", result.Description);
            Assert.Contains("payments", result.Description);
        }

        [Fact]
        public async Task CheckHealthAsync_AfterDispose_ShouldReportUnhealthy()
        {
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System)));
            var factory = (OutboxStoreFactory)provider.GetRequiredService<IOutboxStoreFactory>();
            factory.Dispose();

            var result = await factory.CheckHealthAsync(new HealthCheckContext());

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
        }

        [Fact]
        public void AddOutboxStore_ShouldRegisterTheConcreteFactoryTypeForHealthCheckWiring()
        {
            // services.AddHealthChecks().AddCheck<OutboxStoreFactory>(...) needs the concrete type
            // itself resolvable, not just the IOutboxStoreFactory interface.
            var provider = BuildProvider(services => services.AddOutboxStore("orders", OutboxStoreType.InMemory, _ => new ReferenceOutboxStore(TimeProvider.System)));

            var concrete = provider.GetRequiredService<OutboxStoreFactory>();
            var viaInterface = provider.GetRequiredService<IOutboxStoreFactory>();

            Assert.Same(viaInterface, concrete);
        }

        private static ServiceProvider BuildProvider(Action<IServiceCollection> configure)
        {
            var services = new ServiceCollection();
            configure(services);
            return services.BuildServiceProvider();
        }

        private sealed class DisposableTestStore : IOutboxStore, IDisposable, IAsyncDisposable
        {
            public bool Disposed { get; private set; }
            public bool AsyncDisposed { get; private set; }

            public void Dispose() => Disposed = true;

            public ValueTask DisposeAsync()
            {
                AsyncDisposed = true;
                return ValueTask.CompletedTask;
            }

            public Task<OutboxMessage> EnqueueAsync(OutboxEnqueueRequest request, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string? partitionKey, int maxCount, string leaseOwner, TimeSpan leaseDuration, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<IReadOnlyList<string?>> GetClaimablePartitionKeysAsync(CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<OutboxMessage?> RenewLeaseAsync(Guid id, string leaseOwner, TimeSpan leaseExtension, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<OutboxMessage?> MarkPublishedAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<OutboxMessage?> MarkFailedAsync(Guid id, string leaseOwner, string failureReason, DateTimeOffset? nextRetryAtUtc, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<OutboxMessage?> MarkDeadLetterAsync(Guid id, string leaseOwner, string failureReason, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<bool> ReleaseAsync(Guid id, string leaseOwner, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<int> GetDepthAsync(string? partitionKey, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<TimeSpan?> GetOldestPendingAgeAsync(string? partitionKey, TimeProvider timeProvider, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<OutboxPurgeResult> PurgeAsync(OutboxPurgeRequest request, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();

            public Task<OutboxMessage?> ReplayAsync(Guid id, CancellationToken cancellationToken = default) =>
                throw new NotSupportedException();
        }
    }
}
