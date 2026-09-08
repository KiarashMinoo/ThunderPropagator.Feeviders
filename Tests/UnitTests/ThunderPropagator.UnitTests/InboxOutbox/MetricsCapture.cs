using System.Diagnostics.Metrics;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Captures every measurement recorded to instruments on one named <see cref="Meter"/>, using the
    /// BCL's own <see cref="MeterListener"/> - no extra package needed. Call <see cref="RecordObservableInstruments"/>
    /// to force an <see cref="ObservableGauge{T}"/> callback to run once (it otherwise only runs when a
    /// collector scrapes it).
    /// </summary>
    internal sealed class MetricsCapture : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly object _gate = new();
        private readonly List<(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags)> _measurements = [];

        /// <summary>
        /// A snapshot, not a live view - <see cref="System.Diagnostics.Metrics"/> measurement callbacks
        /// are synchronous but not guaranteed to run on the caller's thread (an awaited async continuation
        /// resumes wherever the thread pool schedules it), so concurrent recordings genuinely race on the
        /// backing list; every access here is under <see cref="_gate"/>.
        /// </summary>
        public IReadOnlyList<(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags)> Measurements
        {
            get { lock (_gate) return [.. _measurements]; }
        }

        public MetricsCapture(string meterName)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == meterName)
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument.Name, value, tags));
            _listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Record(instrument.Name, value, tags));
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument.Name, value, tags));
            _listener.Start();
        }

        public void RecordObservableInstruments() => _listener.RecordObservableInstruments();

        private void Record(string name, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var tagDict = new Dictionary<string, object?>();
            foreach (var tag in tags)
                tagDict[tag.Key] = tag.Value;

            lock (_gate)
                _measurements.Add((name, value, tagDict));
        }

        public void Dispose() => _listener.Dispose();
    }
}
