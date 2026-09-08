namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Thrown by <see cref="OutboxDeadLetterReplayService.ReplayAsync"/> when a replay was denied by
    /// <see cref="IOutboxReplayAuthorizer"/>, or when the entry was not (or no longer)
    /// <see cref="OutboxMessageStatus.DeadLettered"/> by the time the replay itself was attempted - the
    /// audit entry is always written (see <see cref="IOutboxReplayAuditSink"/>) before this is thrown, so
    /// a caller catching this has already been recorded.
    /// </summary>
    public sealed class OutboxDeadLetterReplayException : Exception
    {
        public OutboxDeadLetterReplayException(string message)
            : base(message)
        {
        }
    }
}
