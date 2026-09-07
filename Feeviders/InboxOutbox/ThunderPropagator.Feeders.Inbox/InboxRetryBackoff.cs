namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Computes the delay before <see cref="InboxRetryWorker"/> next attempts a Failed entry - full
    /// jitter over an exponential curve (<c>attempt-1</c> doublings of <see cref="InboxOptions.RetryBaseDelay"/>,
    /// capped at <see cref="InboxOptions.RetryMaxDelay"/>, then a uniformly random delay somewhere in
    /// <c>[0, cappedDelay]</c>). Jitter spreads out entries that became retry-eligible at the same moment
    /// (e.g. a batch of leases that all expired together after a worker crash) instead of every one of
    /// them retrying in lockstep and re-contending for the same claim at once.
    /// </summary>
    public static class InboxRetryBackoff
    {
        /// <param name="options">Supplies <see cref="InboxOptions.RetryBaseDelay"/>/<see cref="InboxOptions.RetryMaxDelay"/>.</param>
        /// <param name="attemptCount">The entry's <see cref="InboxMessage.AttemptCount"/> after the attempt that just failed. Values below 1 are treated as 1.</param>
        /// <param name="random">
        /// Source of jitter. Tests should pass a seeded <see cref="Random"/> for determinism; production
        /// callers can pass <see cref="Random.Shared"/>.
        /// </param>
        public static TimeSpan Compute(InboxOptions options, int attemptCount, Random random)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(random);

            var exponent = Math.Min(Math.Max(0, attemptCount - 1), 30);
            var factor = Math.Pow(2, exponent);
            var uncappedTicks = options.RetryBaseDelay.Ticks * factor;

            var cappedDelay = uncappedTicks >= options.RetryMaxDelay.Ticks || double.IsInfinity(uncappedTicks)
                ? options.RetryMaxDelay
                : TimeSpan.FromTicks((long)uncappedTicks);

            if (cappedDelay <= TimeSpan.Zero)
                return TimeSpan.Zero;

            var jitteredTicks = (long)(random.NextDouble() * cappedDelay.Ticks);
            return TimeSpan.FromTicks(jitteredTicks);
        }
    }
}
