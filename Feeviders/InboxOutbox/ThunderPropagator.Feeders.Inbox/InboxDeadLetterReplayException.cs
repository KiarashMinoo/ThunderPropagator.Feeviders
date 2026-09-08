namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Thrown by <see cref="InboxDeadLetterReplayService.ReplayAsync"/> when a replay was denied by
    /// <see cref="IInboxReplayAuthorizer"/>, or when the entry was not (or no longer)
    /// <see cref="InboxMessageStatus.DeadLettered"/> by the time the replay itself was attempted - the
    /// audit entry is always written (see <see cref="IInboxReplayAuditSink"/>) before this is thrown, so
    /// a caller catching this has already been recorded.
    /// </summary>
    public sealed class InboxDeadLetterReplayException : Exception
    {
        public InboxDeadLetterReplayException(string message)
            : base(message)
        {
        }
    }
}
