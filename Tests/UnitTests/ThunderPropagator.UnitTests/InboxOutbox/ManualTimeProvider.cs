namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>A settable <see cref="TimeProvider"/> shared by the Inbox tests that need to control the clock.</summary>
    internal sealed class ManualTimeProvider(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset _now = now;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}
