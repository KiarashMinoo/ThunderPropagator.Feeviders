namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Exposes when a background Inbox worker (<see cref="InboxRetryWorker"/>) last completed a poll
    /// iteration for a channel - <see cref="InboxHealthCheck"/>'s only way to tell "the worker is stuck
    /// or was never started" apart from "there is simply nothing to retry right now", since both look
    /// identical from the store's own state alone.
    /// </summary>
    public interface IInboxWorkerHeartbeat
    {
        /// <summary>
        /// The UTC time each channel's polling loop last finished an iteration, whether or not that
        /// iteration found anything to reprocess. A channel absent from this dictionary has never
        /// completed one - either polling has not started yet, or it is hung mid-iteration.
        /// </summary>
        IReadOnlyDictionary<Guid, DateTimeOffset> LastPolledAtUtc { get; }
    }
}
