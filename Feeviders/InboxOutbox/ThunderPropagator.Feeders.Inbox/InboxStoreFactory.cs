using System.Collections.Concurrent;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Default <see cref="IInboxStoreFactory"/>. Registered as a singleton by
    /// <see cref="InboxStoreServiceCollectionExtensions.AddInboxStore"/> so every named backend is
    /// resolved/constructed once for the process lifetime and shared by every caller - the lifetime a
    /// pooled client connection (Redis/Mongo/a <c>DbContext</c> factory) needs, not a per-message one.
    /// Disposes every store instance it actually created (never one that was registered but never
    /// resolved) when the factory itself is disposed, which happens when the root
    /// <see cref="IServiceProvider"/> is disposed.
    /// </summary>
    public sealed class InboxStoreFactory : IInboxStoreFactory, IDisposable, IAsyncDisposable
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IReadOnlyDictionary<string, InboxStoreRegistration> _registrations;
        private readonly ConcurrentDictionary<string, Lazy<IInboxStore>> _createdStores = new(StringComparer.Ordinal);
        private int _disposed;

        public InboxStoreFactory(IServiceProvider serviceProvider, IEnumerable<InboxStoreRegistration> registrations)
        {
            _serviceProvider = serviceProvider;
            _registrations = BuildRegistrationLookup(registrations);
        }

        /// <inheritdoc/>
        public IInboxStore GetStore(string storeName, InboxStoreType expectedStoreType)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            if (!_registrations.TryGetValue(storeName, out var registration))
                throw new InvalidOperationException($"No Inbox store named '{storeName}' is registered.");

            if (registration.StoreType != expectedStoreType)
                throw new InvalidOperationException(
                    $"Inbox store '{storeName}' is registered as {registration.StoreType}, but {expectedStoreType} was expected.");

            // GetOrAdd may construct-and-discard a Lazy<T> under contention, but Lazy<T>'s own
            // ExecutionAndPublication mode still guarantees registration.CreateStore runs at most once -
            // only the discarded wrapper, never the store itself, is ever duplicated.
            var lazyStore = _createdStores.GetOrAdd(
                storeName,
                _ => new Lazy<IInboxStore>(() => registration.CreateStore(_serviceProvider), LazyThreadSafetyMode.ExecutionAndPublication));

            return lazyStore.Value;
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

        private static IReadOnlyDictionary<string, InboxStoreRegistration> BuildRegistrationLookup(IEnumerable<InboxStoreRegistration> registrations)
        {
            var lookup = new Dictionary<string, InboxStoreRegistration>(StringComparer.Ordinal);

            foreach (var registration in registrations)
            {
                if (!lookup.TryAdd(registration.StoreName, registration))
                    throw new InvalidOperationException($"An Inbox store named '{registration.StoreName}' is already registered.");
            }

            return lookup;
        }
    }
}
