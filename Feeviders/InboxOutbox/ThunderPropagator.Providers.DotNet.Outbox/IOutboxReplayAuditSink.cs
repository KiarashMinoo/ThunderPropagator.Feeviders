namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Durably records every replay attempt <see cref="OutboxDeadLetterReplayService"/> makes - the
    /// audit trail acceptance criterion. See <c>IInboxReplayAuditSink</c>'s remarks: this library defines
    /// the record shape and the guarantee that every attempt is recorded, not the storage itself.
    /// </summary>
    public interface IOutboxReplayAuditSink
    {
        /// <summary>
        /// Records <paramref name="entry"/>. Called for every replay attempt - denied, failed, or
        /// succeeded - never only for successes.
        /// </summary>
        Task RecordAsync(OutboxReplayAuditEntry entry, CancellationToken cancellationToken);
    }
}
