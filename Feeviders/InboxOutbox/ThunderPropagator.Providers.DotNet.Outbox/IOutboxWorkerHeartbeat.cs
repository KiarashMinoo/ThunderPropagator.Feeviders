namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Exposes when a background Outbox worker (<c>OutboxRelayWorker</c>) last completed a poll
    /// iteration for a Provider - <see cref="OutboxHealthCheck"/>'s only way to tell "the worker is stuck
    /// or was never started" apart from "there is simply nothing to relay right now", since both look
    /// identical from the store's own state alone.
    /// </summary>
    public interface IOutboxWorkerHeartbeat
    {
        /// <summary>
        /// The UTC time each Provider's polling loop last finished an iteration, whether or not that
        /// iteration found anything to relay. A Provider absent from this dictionary has never completed
        /// one - either polling has not started yet, or it is hung mid-iteration.
        /// </summary>
        IReadOnlyDictionary<string, DateTimeOffset> LastPolledAtUtc { get; }
    }
}
