using System.Diagnostics;

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

            // "outbox.enqueue" is anchored to this call, not to CommitAsync's later store write - it
            // represents the caller's own decision to enqueue, so it naturally nests under whatever
            // business-transaction activity is ambient here. Its own trace context (not its parent's) is
            // what gets persisted, so a relay attempt links back to this exact span.
            using var activity = OutboxTelemetry.ActivitySource.StartActivity("outbox.enqueue", ActivityKind.Internal);
            activity?.SetTag("outbox.provider_key", request.ProviderKey);
            activity?.SetTag("outbox.message_id", request.MessageId);

            _staged.Add(request with { Headers = OutboxTraceContext.WithCurrentTraceContext(request.Headers) });
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
