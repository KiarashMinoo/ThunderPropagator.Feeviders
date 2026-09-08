using System.Diagnostics;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Captures every completed <see cref="Activity"/> started on one named <see cref="ActivitySource"/>,
    /// using the BCL's own <see cref="ActivityListener"/> - no extra package needed. Activities are
    /// recorded on <see cref="ActivityStopped"/> (not started) so every tag/status the operation sets
    /// during its own lifetime is already present by the time a test inspects it.
    /// </summary>
    internal sealed class ActivityCapture : IDisposable
    {
        private readonly ActivityListener _listener;
        private readonly object _gate = new();
        private readonly List<Activity> _activities = [];

        public IReadOnlyList<Activity> Activities
        {
            get { lock (_gate) return [.. _activities]; }
        }

        public ActivityCapture(string activitySourceName)
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == activitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    lock (_gate)
                        _activities.Add(activity);
                },
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public void Dispose() => _listener.Dispose();
    }
}
