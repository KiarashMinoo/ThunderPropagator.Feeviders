namespace ThunderPropagator.Feeders.Grpc
{
    internal static class GrpcReconnectDelay
    {
        public static TimeSpan Calculate(TimeSpan initialDelay, TimeSpan maximumDelay, int attempt)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(initialDelay, TimeSpan.Zero);
            ArgumentOutOfRangeException.ThrowIfLessThan(maximumDelay, initialDelay);
            ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);

            var exponent = Math.Min(attempt - 1, 30);
            var delayTicks = initialDelay.Ticks * Math.Pow(2, exponent);

            return TimeSpan.FromTicks((long)Math.Min(delayTicks, maximumDelay.Ticks));
        }
    }
}
