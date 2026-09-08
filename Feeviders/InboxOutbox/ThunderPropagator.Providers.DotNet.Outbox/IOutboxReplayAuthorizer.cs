namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Decides whether a manual replay/requeue of a dead-lettered entry (<see cref="OutboxDeadLetterReplayService"/>)
    /// may proceed. Required (no default "allow all" implementation is provided) - a caller must decide,
    /// deliberately, who is allowed to requeue a dead letter back into live publishing.
    /// </summary>
    public interface IOutboxReplayAuthorizer
    {
        /// <summary>
        /// Authorizes <paramref name="requestedBy"/> to replay <paramref name="message"/> (already
        /// confirmed <see cref="OutboxMessageStatus.DeadLettered"/> by the caller).
        /// </summary>
        Task<OutboxReplayAuthorizationResult> AuthorizeAsync(OutboxMessage message, string requestedBy, CancellationToken cancellationToken);
    }
}
