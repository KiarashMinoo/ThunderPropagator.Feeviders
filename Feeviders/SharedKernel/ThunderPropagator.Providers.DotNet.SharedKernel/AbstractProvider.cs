using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using System.Diagnostics;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.CorrelationId;
using ThunderPropagator.BuildingBlocks.Application.Objects;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    public abstract class AbstractProvider<TFeederMessage, TProviderConfiguration> : DisposableObject,
        IProvider<TFeederMessage>
        where TFeederMessage : FeederMessage
        where TProviderConfiguration : class, IAbstractProviderConfiguration
    {
        private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

        private readonly IFeederMessageSerializer<TFeederMessage, TProviderConfiguration> _feederMessageSerializer;
        private readonly IOutboxStoreFactory? _outboxStoreFactory;

        protected ILogger Logger { get; }

        /// <summary>The configuration this Provider was constructed with - the source of <see cref="IAbstractProviderConfiguration.Outbox"/>.</summary>
        protected TProviderConfiguration ProviderConfiguration { get; }

        /// <exception cref="ArgumentException"><see cref="IAbstractProviderConfiguration.Outbox"/> is enabled but fails <see cref="OutboxOptions.Validate"/>.</exception>
        /// <exception cref="InvalidOperationException"><see cref="IAbstractProviderConfiguration.Outbox"/> is enabled but no <see cref="IOutboxStoreFactory"/> is registered, or has no <see cref="OutboxOptions.StoreConnectionName"/>.</exception>
        protected AbstractProvider(TProviderConfiguration providerConfiguration, IServiceProvider serviceProvider)
        {
            ArgumentNullException.ThrowIfNull(providerConfiguration);
            ArgumentNullException.ThrowIfNull(serviceProvider);

            ProviderConfiguration = providerConfiguration;
            Logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger(GetType());
            _feederMessageSerializer = serviceProvider.GetRequiredService<IFeederMessageSerializer<TFeederMessage, TProviderConfiguration>>();
            _outboxStoreFactory = serviceProvider.GetService<IOutboxStoreFactory>();

            if (!providerConfiguration.Outbox.OutboxEnabled)
                return;

            providerConfiguration.Outbox.Validate();

            if (string.IsNullOrWhiteSpace(providerConfiguration.Outbox.StoreConnectionName))
                throw new InvalidOperationException(
                    $"{nameof(OutboxOptions.StoreConnectionName)} must be set to resolve a registered Outbox store when {nameof(OutboxOptions.OutboxEnabled)} is true.");

            if (_outboxStoreFactory is null)
                throw new InvalidOperationException(
                    $"Outbox is enabled but no {nameof(IOutboxStoreFactory)} is registered. Call {nameof(OutboxStoreServiceCollectionExtensions.AddOutboxStore)} during service registration.");
        }

        /// <inheritdoc cref="IProvider{TFeederMessage}.ExecuteAsync"/>
        public async Task ExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default) =>
            await ExecuteCoreAsync(feederMessage, cancellationToken).ConfigureAwait(false);

        /// <summary>
        /// Same behavior as <see cref="ExecuteAsync"/>, but also reports whether the message was
        /// published directly or durably enqueued - see <see cref="ProviderExecuteResult"/>.
        /// </summary>
        public Task<ProviderExecuteResult> ExecuteWithResultAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default) =>
            ExecuteCoreAsync(feederMessage, cancellationToken);

        private async Task<ProviderExecuteResult> ExecuteCoreAsync(TFeederMessage feederMessage, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(feederMessage);
            feederMessage.TryAdd("PublishedDateTime", DateTime.UtcNow);

            if (!ProviderConfiguration.Outbox.OutboxEnabled)
            {
                await InternalExecuteAsync(feederMessage, cancellationToken).ConfigureAwait(false);
                return ProviderExecuteResult.Published();
            }

            return await EnqueueAsync(feederMessage, cancellationToken).ConfigureAwait(false);
        }

        private async Task<ProviderExecuteResult> EnqueueAsync(TFeederMessage feederMessage, CancellationToken cancellationToken)
        {
            var options = ProviderConfiguration.Outbox;

            // Serialize once - the same bytes become both the durable OutboxMessage.Payload and,
            // eventually, exactly what a relay worker hands to PublishDirectAsync.
            var bytes = _feederMessageSerializer.SerializeToBytes(feederMessage, cancellationToken);

            if (string.IsNullOrWhiteSpace(feederMessage.CorrelationId))
                CorrelationIdSupportHelper.GenerateCorrelationId(feederMessage);

            var store = _outboxStoreFactory!.GetStore(options.StoreConnectionName!, options.StoreType);
            await using var unitOfWork = CreateOutboxUnitOfWork(options, store);

            unitOfWork.Enqueue(new OutboxEnqueueRequest
            {
                MessageId = feederMessage.CorrelationId,
                ProviderKey = ProviderConfiguration.Id.ToString(),
                PartitionKey = ResolvePartitionKey(feederMessage, options),
                SchemaVersion = 1,
                PayloadContentType = ProviderConfiguration.SerializerType.ToString(),
                Payload = bytes,
                Headers = BuildHeaders(),
            });

            var committed = await unitOfWork.CommitAsync(cancellationToken).ConfigureAwait(false);
            return ProviderExecuteResult.Enqueued(committed[0]);
        }

        private static string? ResolvePartitionKey(TFeederMessage feederMessage, OutboxOptions options) =>
            options.PartitionStrategy switch
            {
                OutboxPartitionStrategy.Fixed => options.FixedPartitionKey,
                // The dictionary-backed FeederMessage's public indexer only exposes a getter (its
                // setter is protected), so a caller that wants CallerSupplied partitioning sets this
                // well-known key through whatever means its own TFeederMessage subclass allows -
                // mirroring how AbstractProvider itself already sets "PublishedDateTime" the same way.
                OutboxPartitionStrategy.CallerSupplied => feederMessage["OutboxPartitionKey"]?.ToString(),
                _ => null,
            };

        /// <summary>
        /// Propagates the current trace context/baggage into the Outbox entry's headers, so a relay
        /// worker publishing this message later (in a different process, after an arbitrary delay) can
        /// restore the originating trace instead of starting an unrelated one.
        /// </summary>
        private static IReadOnlyDictionary<string, string>? BuildHeaders()
        {
            if (Activity.Current?.Context is not { } context)
                return null;

            var headers = new Dictionary<string, string>();
            Propagator.Inject(new PropagationContext(context, Baggage.Current), headers, static (carrier, key, value) => carrier[key] = value);
            return headers.Count == 0 ? null : headers;
        }

        /// <summary>
        /// Builds the unit of work <see cref="EnqueueAsync"/> stages the message on. The default
        /// (<see cref="OutboxTransactionMode.NonTransactional"/>) needs only <paramref name="store"/>.
        /// <see cref="OutboxTransactionMode.Enlisted"/> needs a caller-supplied <c>DbContext</c> this
        /// generic base type has no way to obtain on its own - override this method to construct an
        /// <c>EfCoreOutboxUnitOfWork</c> around whatever <c>DbContext</c> carries the ambient business
        /// transaction (e.g. resolved from an <see cref="IServiceProvider"/> scope).
        /// </summary>
        protected virtual IOutboxUnitOfWork CreateOutboxUnitOfWork(OutboxOptions options, IOutboxStore store) =>
            options.TransactionMode switch
            {
                OutboxTransactionMode.NonTransactional => new NonTransactionalOutboxUnitOfWork(store),
                _ => throw new InvalidOperationException(
                    $"{nameof(OutboxTransactionMode)}.{options.TransactionMode} has no default {nameof(IOutboxUnitOfWork)} - override {nameof(CreateOutboxUnitOfWork)}."),
            };

        /// <inheritdoc/>
        /// <remarks>
        /// <paramref name="headers"/> is ignored - <see cref="InternalExecuteAsync(byte[], CancellationToken)"/>
        /// has no way to attach transport-level headers to a raw-bytes publish. A transport that gains
        /// one can override this member directly to honor it.
        /// </remarks>
        Task IProvider.PublishDirectAsync(byte[] bytes, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken) =>
            InternalExecuteAsync(bytes, cancellationToken);

        protected virtual Task InternalExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default)
        {
            return InternalExecuteAsync(_feederMessageSerializer.SerializeToBytes(feederMessage, cancellationToken), cancellationToken);
        }

        protected abstract Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default);
    }
}
