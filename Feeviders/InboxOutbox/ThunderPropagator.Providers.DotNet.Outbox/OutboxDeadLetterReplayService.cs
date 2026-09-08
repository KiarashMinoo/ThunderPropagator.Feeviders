namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// The manual replay/requeue entry point for a dead-lettered Outbox entry - "explicit, audited, and
    /// idempotent": explicit because a caller must invoke this deliberately; audited because every
    /// attempt, authorized or not, successful or not, is recorded via <see cref="IOutboxReplayAuditSink"/>
    /// before this method returns or throws; idempotent because <see cref="IOutboxStore.ReplayAsync"/>
    /// only ever applies to an entry still <see cref="OutboxMessageStatus.DeadLettered"/> at the moment
    /// of the atomic transition - a second, concurrent, or repeated replay request against the same
    /// already-replayed entry fails rather than requeuing it twice.
    /// </summary>
    public sealed class OutboxDeadLetterReplayService(IOutboxStore store, IOutboxReplayAuthorizer authorizer, IOutboxReplayAuditSink auditSink, TimeProvider? timeProvider = null)
    {
        private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;

        /// <summary>
        /// Replays <paramref name="message"/> - which the caller must have already fetched and confirmed
        /// is <see cref="OutboxMessageStatus.DeadLettered"/> (e.g. from a dead-letter listing UI/API) -
        /// resetting its attempt count and assigning it a new ordering-sequence identity via
        /// <see cref="OutboxMessage.Requeue"/>. Returns the new <see cref="OutboxMessageStatus.Pending"/>
        /// snapshot.
        /// </summary>
        /// <param name="message">The dead-lettered snapshot to replay.</param>
        /// <param name="requestedBy">Opaque identity of whoever is requesting the replay - recorded on the audit entry, never interpreted.</param>
        /// <param name="cancellationToken"></param>
        /// <exception cref="OutboxDeadLetterReplayException">
        /// <paramref name="requestedBy"/> was not authorized by <see cref="IOutboxReplayAuthorizer"/>, or
        /// <paramref name="message"/> was not (or was no longer) DeadLettered when the replay itself ran.
        /// </exception>
        public async Task<OutboxMessage> ReplayAsync(OutboxMessage message, string requestedBy, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(message);
            ArgumentException.ThrowIfNullOrWhiteSpace(requestedBy);

            var requestedAtUtc = _timeProvider.GetUtcNow();

            if (message.Status != OutboxMessageStatus.DeadLettered)
            {
                await auditSink.RecordAsync(new OutboxReplayAuditEntry
                {
                    Id = message.Id,
                    MessageId = message.MessageId,
                    ProviderKey = message.ProviderKey,
                    RequestedBy = requestedBy,
                    RequestedAtUtc = requestedAtUtc,
                    Authorized = false,
                    DenialReason = $"Entry is {message.Status}, not {OutboxMessageStatus.DeadLettered}.",
                    Succeeded = false,
                }, cancellationToken).ConfigureAwait(false);

                throw new OutboxDeadLetterReplayException($"Entry '{message.Id}' is {message.Status}, not {OutboxMessageStatus.DeadLettered} - it cannot be replayed.");
            }

            var authorization = await authorizer.AuthorizeAsync(message, requestedBy, cancellationToken).ConfigureAwait(false);

            if (!authorization.IsAuthorized)
            {
                await auditSink.RecordAsync(new OutboxReplayAuditEntry
                {
                    Id = message.Id,
                    MessageId = message.MessageId,
                    ProviderKey = message.ProviderKey,
                    RequestedBy = requestedBy,
                    RequestedAtUtc = requestedAtUtc,
                    Authorized = false,
                    DenialReason = authorization.DenialReason,
                    Succeeded = false,
                }, cancellationToken).ConfigureAwait(false);

                throw new OutboxDeadLetterReplayException($"Replay of entry '{message.Id}' was not authorized: {authorization.DenialReason}");
            }

            var replayed = await store.ReplayAsync(message.Id, cancellationToken).ConfigureAwait(false);

            await auditSink.RecordAsync(new OutboxReplayAuditEntry
            {
                Id = message.Id,
                MessageId = message.MessageId,
                ProviderKey = message.ProviderKey,
                RequestedBy = requestedBy,
                RequestedAtUtc = requestedAtUtc,
                Authorized = true,
                Succeeded = replayed is not null,
                FailureReason = replayed is null ? "Entry was no longer DeadLettered by the time the replay was applied." : null,
            }, cancellationToken).ConfigureAwait(false);

            return replayed ?? throw new OutboxDeadLetterReplayException(
                $"Entry '{message.Id}' was no longer {OutboxMessageStatus.DeadLettered} by the time the replay was applied - it may have already been replayed.");
        }
    }
}
