namespace ThunderPropagator.Feeders.SharedKernel
{
    internal sealed class InFlightMessageTracker
    {
        private readonly object _sync = new();
        private TaskCompletionSource _drained = CreateCompletedSource();
        private int _count;
        private bool _isDraining;

        public bool TryBegin()
        {
            lock (_sync)
            {
                if (_isDraining)
                    return false;

                if (_count++ == 0)
                    _drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

                return true;
            }
        }

        public void Complete()
        {
            lock (_sync)
            {
                if (--_count == 0)
                    _drained.TrySetResult();
            }
        }

        public async Task DrainAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            Task drainedTask;
            lock (_sync)
            {
                _isDraining = true;
                drainedTask = _drained.Task;
            }

            await Task.WhenAny(drainedTask, Task.Delay(timeout, cancellationToken)).ConfigureAwait(false);
        }

        private static TaskCompletionSource CreateCompletedSource()
        {
            var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            source.SetResult();
            return source;
        }
    }
}
