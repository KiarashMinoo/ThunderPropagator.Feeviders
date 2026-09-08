namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Decides whether a manual replay of a dead-lettered entry (<see cref="InboxDeadLetterReplayService"/>)
    /// may proceed. Required (no default "allow all" implementation is provided) - a caller must decide,
    /// deliberately, who is allowed to replay a dead letter back into live processing.
    /// </summary>
    public interface IInboxReplayAuthorizer
    {
        /// <summary>
        /// Authorizes <paramref name="requestedBy"/> to replay <paramref name="message"/> (already
        /// confirmed <see cref="InboxMessageStatus.DeadLettered"/> by the caller).
        /// </summary>
        Task<InboxReplayAuthorizationResult> AuthorizeAsync(InboxMessage message, string requestedBy, CancellationToken cancellationToken);
    }
}
