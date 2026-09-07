using System.Collections.Concurrent;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Default <see cref="IOutboxStoreFactory"/>. Registered as a singleton by
    /// <see cref="OutboxStoreServiceCollectionExtensions.AddOutboxStore"/> so every named backend is
    /// resolved/constructed once for the process lifetime and shared by every caller - the lifetime a
    /// pooled client connection (Redis/Mongo/a <c>DbContext</c> factory) needs, not a per-message one.
    /// Disposes every store instance it actually created (never one that was registered but never
    /// resolved) when the factory itself is disposed, which happens when the root
    /// <see cref="IServiceProvider"/> is disposed. Also implements <see cref="IHealthCheck"/> so a host
    /// that registers Microsoft.Extensions.Diagnostics.HealthChecks health checks can wire this factory
    /// in directly (e.g. <c>services.AddHealthChecks().AddCheck&lt;OutboxStoreFactory&gt;(name)</c>) -
    /// this only reports the factory's own lifecycle state (disposed or not), not per-backend
    /// connectivity, since no real backend exists yet to probe (see the Storage Backends epic).
    /// </summary>
    public sealed class OutboxStoreFactory : IOutboxStoreFactory, IDisposable, IAsyncDisposable, IHealthCheck
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IReadOnlyDictionary<string, OutboxStoreRegistration> _registrations;
        private readonly ConcurrentDictionary<string, Lazy<IOutboxStore>> _createdStores = new(StringComparer.Ordinal);
        private int _disposed;

        public OutboxStoreFactory(IServiceProvider serviceProvider, IEnumerable<OutboxStoreRegistration> registrations)
        {
            _serviceProvider = serviceProvider;
            _registrations = BuildRegistrationLookup(registrations);
        }

        /// <inheritdoc/>
        public IOutboxStore GetStore(string storeName, OutboxStoreType expectedStoreType)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            if (!_registrations.TryGetValue(storeName, out var registration))
                throw new InvalidOperationException($"No Outbox store named '{storeName}' is registered.");

            if (registration.StoreType != expectedStoreType)
                throw new InvalidOperationException(
                    $"Outbox store '{storeName}' is registered as {registration.StoreType}, but {expectedStoreType} was expected.");

            // GetOrAdd may construct-and-discard a Lazy<T> under contention, but Lazy<T>'s own
            // ExecutionAndPublication mode still guarantees registration.CreateStore runs at most once -
            // only the discarded wrapper, never the store itself, is ever duplicated.
            var lazyStore = _createdStores.GetOrAdd(
                storeName,
                _ => new Lazy<IOutboxStore>(() => registration.CreateStore(_serviceProvider), LazyThreadSafetyMode.ExecutionAndPublication));

            return lazyStore.Value;
        }

        /// <inheritdoc/>
        public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var result = Volatile.Read(ref _disposed) != 0
                ? HealthCheckResult.Unhealthy($"{nameof(OutboxStoreFactory)} has been disposed.")
                : HealthCheckResult.Healthy($"{_registrations.Count} Outbox store(s) registered: {string.Join(", ", _registrations.Keys)}.");

            return Task.FromResult(result);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            foreach (var lazyStore in _createdStores.Values)
            {
                if (lazyStore.IsValueCreated && lazyStore.Value is IDisposable disposable)
                    disposable.Dispose();
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            foreach (var lazyStore in _createdStores.Values)
            {
                if (!lazyStore.IsValueCreated)
                    continue;

                switch (lazyStore.Value)
                {
                    case IAsyncDisposable asyncDisposable:
                        await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                        break;
                    case IDisposable disposable:
                        disposable.Dispose();
                        break;
                }
            }
        }

        private static IReadOnlyDictionary<string, OutboxStoreRegistration> BuildRegistrationLookup(IEnumerable<OutboxStoreRegistration> registrations)
        {
            var lookup = new Dictionary<string, OutboxStoreRegistration>(StringComparer.Ordinal);

            foreach (var registration in registrations)
            {
                if (!lookup.TryAdd(registration.StoreName, registration))
                    throw new InvalidOperationException($"An Outbox store named '{registration.StoreName}' is already registered.");
            }

            return lookup;
        }
    }
}
