namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// The <see cref="OutboxTransactionMode.NonTransactional"/> <see cref="IOutboxUnitOfWork"/>: a
    /// durable buffer, not a true Transactional Outbox. <see cref="CommitAsync"/> enqueues every staged
    /// message directly through <see cref="IOutboxStore.EnqueueAsync"/>, one at a time, with no shared
    /// transaction - neither with business state, nor across the staged messages themselves. If a
    /// business-side write already committed (or commits later) independently of this call, or if this
    /// call fails partway through a multi-message commit, the two can disagree: business state may exist
    /// without its corresponding message ever being enqueued, or vice versa.
    /// </summary>
    /// <remarks>
    /// Appropriate only for backends <see cref="OutboxOptions.Validate"/> would reject
    /// <see cref="OutboxTransactionMode.Enlisted"/> for (Redis, MongoDB, InMemory - see
    /// <see cref="OutboxStoreType"/>), or for an EF Core backend where the caller has deliberately opted
    /// out of enlistment. Choosing this mode is an explicit acknowledgement that at-least-once delivery
    /// (not exactly-once, not guaranteed) is the only guarantee in play.
    /// </remarks>
    public sealed class NonTransactionalOutboxUnitOfWork(IOutboxStore store) : IOutboxUnitOfWork
    {
        private readonly List<OutboxEnqueueRequest> _staged = [];

        /// <inheritdoc/>
        public void Enqueue(OutboxEnqueueRequest request)
        {
            ArgumentNullException.ThrowIfNull(request);
            _staged.Add(request);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<OutboxMessage>> CommitAsync(CancellationToken cancellationToken = default)
        {
            var enqueued = new List<OutboxMessage>(_staged.Count);

            foreach (var request in _staged)
            {
                var message = await store.EnqueueAsync(request, cancellationToken).ConfigureAwait(false);
                enqueued.Add(message);
                OutboxTelemetry.Enqueued.Add(1, new KeyValuePair<string, object?>(OutboxTelemetry.TagProvider, message.ProviderKey));
            }

            _staged.Clear();
            return enqueued;
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            _staged.Clear();
            return ValueTask.CompletedTask;
        }
    }
}
