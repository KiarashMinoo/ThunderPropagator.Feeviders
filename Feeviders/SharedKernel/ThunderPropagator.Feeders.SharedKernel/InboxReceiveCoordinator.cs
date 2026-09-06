using System.Diagnostics;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.Feeders.SharedKernel
{
    /// <summary>
    /// Wraps one Feevider's receive path with atomic Inbox processing, without requiring any change to
    /// <c>DelegativeFeeder</c> itself (that type lives outside this repository). A Feeder constructs one
    /// instance per Feevider - resolving/validating configuration and the backing <see cref="IInboxStore"/>
    /// once, at construction (before consumption starts) - and calls <see cref="ReceiveAsync"/> from its
    /// existing receive handler in place of calling the transport's own acknowledge/handler logic directly.
    /// </summary>
    /// <remarks>
    /// When <see cref="InboxOptions.InboxEnabled"/> is <see langword="false"/>, <see cref="ReceiveAsync"/>
    /// only invokes <c>invokeHandlerAsync</c> and returns <see cref="InboxReceiveOutcome.Disabled"/> -
    /// callers must still perform their own broker acknowledgement in that case, exactly as before this
    /// type existed. When enabled, this type calls <c>acknowledgeAsync</c> itself, immediately after a
    /// successful claim - once a message is durably claimed, the Inbox (and its retry worker) owns
    /// recovering it, so a crash before <see cref="IInboxStore.CompleteAsync"/> is recoverable by lease
    /// expiry rather than by broker redelivery. Callers must not acknowledge again for any outcome other
    /// than <see cref="InboxReceiveOutcome.Disabled"/>.
    /// </remarks>
    public sealed class InboxReceiveCoordinator
    {
        internal static readonly ActivitySource ActivitySource = new("ThunderPropagator.Feeders.SharedKernel.InboxReceiveCoordinator");

        private readonly InboxOptions _options;
        private readonly Guid _channelKey;
        private readonly Guid _feederId;
        private readonly TimeProvider _timeProvider;
        private readonly IInboxStore? _store;
        private readonly MessageIdResolver? _messageIdResolver;

        /// <summary>Whether the Inbox is active - mirrors <see cref="InboxOptions.InboxEnabled"/>.</summary>
        public bool InboxEnabled => _options.InboxEnabled;

        /// <exception cref="ArgumentException">
        /// <paramref name="options"/> fails <see cref="InboxOptions.Validate"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// <paramref name="options"/> has <see cref="InboxOptions.InboxEnabled"/> set but no
        /// <see cref="InboxOptions.StoreConnectionName"/>, or no <paramref name="storeFactory"/> was
        /// supplied - both fail here, at construction, rather than on the first received message.
        /// </exception>
        public InboxReceiveCoordinator(InboxOptions options, Guid channelKey, Guid feederId, IInboxStoreFactory? storeFactory, TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(options);
            options.Validate();

            _options = options;
            _channelKey = channelKey;
            _feederId = feederId;
            _timeProvider = timeProvider ?? TimeProvider.System;

            if (!options.InboxEnabled)
                return;

            if (string.IsNullOrWhiteSpace(options.StoreConnectionName))
                throw new InvalidOperationException(
                    $"{nameof(InboxOptions.StoreConnectionName)} must be set to resolve a registered Inbox store when {nameof(InboxOptions.InboxEnabled)} is true.");

            if (storeFactory is null)
                throw new InvalidOperationException(
                    $"Inbox is enabled but no {nameof(IInboxStoreFactory)} is registered. Call {nameof(InboxStoreServiceCollectionExtensions.AddInboxStore)} during service registration.");

            _store = storeFactory.GetStore(options.StoreConnectionName, options.StoreType);
            _messageIdResolver = new MessageIdResolver(options.MessageIdResolution);
        }

        /// <summary>
        /// Runs the Inbox-wrapped receive path for one inbound message. When disabled, calls
        /// <paramref name="invokeHandlerAsync"/> directly. When enabled: resolves the dedup message ID,
        /// atomically claims (persisting <paramref name="payload"/>/<paramref name="headers"/>),
        /// acknowledges via <paramref name="acknowledgeAsync"/> once claimed, runs
        /// <paramref name="invokeHandlerAsync"/>, and completes/fails the claim based on the outcome.
        /// </summary>
        /// <param name="payload">The raw, original message payload - persisted as-is for a future retry/dead-letter handler.</param>
        /// <param name="payloadContentType">Content type describing <paramref name="payload"/>.</param>
        /// <param name="headers">Broker/transport headers, used by a <see cref="InboxMessageIdStrategy.BrokerHeader"/> resolver and persisted alongside the claim.</param>
        /// <param name="partitionKey">Optional additional dedup/ordering scope beneath channel/feeder - see <see cref="InboxClaimRequest.PartitionKey"/>.</param>
        /// <param name="invokeHandlerAsync">Invokes the Feevider's existing handler path (its own <c>ReceiveAsync</c> call).</param>
        /// <param name="acknowledgeAsync">Acknowledges broker delivery. Called by this method once (immediately after a successful claim, or for a duplicate/already-dead-lettered delivery) - never called by this method for <see cref="InboxReceiveOutcome.InProgress"/>, and must be called by the caller itself for <see cref="InboxReceiveOutcome.Disabled"/>.</param>
        public async ValueTask<InboxReceiveOutcome> ReceiveAsync(
            byte[] payload,
            string payloadContentType,
            IReadOnlyDictionary<string, string>? headers,
            string? partitionKey,
            Func<CancellationToken, ValueTask> invokeHandlerAsync,
            Func<CancellationToken, ValueTask> acknowledgeAsync,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(payload);
            ArgumentNullException.ThrowIfNull(payloadContentType);
            ArgumentNullException.ThrowIfNull(invokeHandlerAsync);
            ArgumentNullException.ThrowIfNull(acknowledgeAsync);

            if (!InboxEnabled)
            {
                await invokeHandlerAsync(cancellationToken).ConfigureAwait(false);
                return InboxReceiveOutcome.Disabled;
            }

            using var activity = ActivitySource.StartActivity("InboxReceiveCoordinator_Receive", ActivityKind.Internal);
            activity?.SetTag("inbox.channel_key", _channelKey);
            activity?.SetTag("inbox.feeder_id", _feederId);

            var resolved = _messageIdResolver!.Resolve(new MessageIdResolutionRequest { Headers = headers, Payload = payload });
            activity?.SetTag("inbox.message_id", resolved.MessageId);

            var leaseOwner = Guid.NewGuid().ToString("N");
            var claim = await _store!.TryClaimAsync(new InboxClaimRequest
            {
                MessageId = resolved.MessageId,
                ChannelKey = _channelKey,
                FeederId = _feederId,
                PartitionKey = partitionKey,
                SchemaVersion = 1,
                PayloadContentType = payloadContentType,
                Payload = payload,
                Headers = headers,
                LeaseOwner = leaseOwner,
                LeaseDuration = _options.ClaimLeaseDuration,
            }, cancellationToken).ConfigureAwait(false);

            activity?.SetTag("inbox.claim_outcome", claim.Outcome.ToString());

            switch (claim.Outcome)
            {
                case InboxClaimOutcome.AlreadyProcessed:
                    await acknowledgeAsync(cancellationToken).ConfigureAwait(false);
                    return InboxReceiveOutcome.Duplicate;

                case InboxClaimOutcome.DeadLettered:
                    await acknowledgeAsync(cancellationToken).ConfigureAwait(false);
                    return InboxReceiveOutcome.AlreadyDeadLettered;

                case InboxClaimOutcome.ClaimedByAnotherOwner:
                    // Another worker holds an unexpired lease on this dedup key - this call never held
                    // the claim, so it must not acknowledge, complete, or fail it.
                    return InboxReceiveOutcome.InProgress;
            }

            var claimed = claim.Message!;

            // Durably claimed: the Inbox (and its retry worker) now owns recovering this message, so the
            // broker is acknowledged now rather than after the handler runs.
            await acknowledgeAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                await invokeHandlerAsync(cancellationToken).ConfigureAwait(false);
                await _store.CompleteAsync(claimed.Id, leaseOwner, cancellationToken).ConfigureAwait(false);
                activity?.SetStatus(ActivityStatusCode.Ok);
                return InboxReceiveOutcome.Processed;
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
                var reason = SanitizeFailureReason(exception);

                if (claimed.AttemptCount >= _options.MaxRetryAttempts)
                {
                    await _store.DeadLetterAsync(claimed.Id, leaseOwner, reason, cancellationToken).ConfigureAwait(false);
                    return InboxReceiveOutcome.DeadLettered;
                }

                var nextRetryAtUtc = _timeProvider.GetUtcNow() + ComputeBackoff(_options, claimed.AttemptCount);
                await _store.FailAsync(claimed.Id, leaseOwner, reason, nextRetryAtUtc, cancellationToken).ConfigureAwait(false);
                return InboxReceiveOutcome.Failed;
            }
        }

        /// <summary>
        /// Never persists <see cref="Exception.Message"/> as-is (it may embed payload content or other
        /// sensitive detail) - only the exception's type name, so operators can triage a Failed/DeadLettered
        /// entry without the Inbox itself becoming a payload/secret log.
        /// </summary>
        private static string SanitizeFailureReason(Exception exception)
        {
            var reason = exception.GetType().FullName ?? exception.GetType().Name;
            return reason.Length > InboxMessageLimits.MaxFailureReasonLength
                ? reason[..InboxMessageLimits.MaxFailureReasonLength]
                : reason;
        }

        private static TimeSpan ComputeBackoff(InboxOptions options, int attemptCount)
        {
            var exponent = Math.Min(Math.Max(0, attemptCount - 1), 30);
            var factor = Math.Pow(2, exponent);
            var delayTicks = options.RetryBaseDelay.Ticks * factor;

            return delayTicks >= options.RetryMaxDelay.Ticks || double.IsInfinity(delayTicks)
                ? options.RetryMaxDelay
                : TimeSpan.FromTicks((long)delayTicks);
        }
    }
}
