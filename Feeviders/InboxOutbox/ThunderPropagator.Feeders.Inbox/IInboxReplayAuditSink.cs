namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Durably records every replay attempt <see cref="InboxDeadLetterReplayService"/> makes - the audit
    /// trail acceptance criterion. A caller supplies an implementation writing to whatever store makes
    /// sense for its own compliance requirements (a database table, an append-only log, etc.); this
    /// library defines the record shape and the guarantee that every attempt is recorded, not the
    /// storage itself.
    /// </summary>
    public interface IInboxReplayAuditSink
    {
        /// <summary>
        /// Records <paramref name="entry"/>. Called for every replay attempt - denied, failed, or
        /// succeeded - never only for successes.
        /// </summary>
        Task RecordAsync(InboxReplayAuditEntry entry, CancellationToken cancellationToken);
    }
}
