using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    public class InFlightMessageTrackerTests
    {
        [Fact]
        public async Task DrainAsync_ShouldWaitForActiveHandlerAndRejectNewWork()
        {
            var tracker = new InFlightMessageTracker();
            Assert.True(tracker.TryBegin());

            var drainTask = tracker.DrainAsync(TimeSpan.FromSeconds(1));

            Assert.False(drainTask.IsCompleted);
            Assert.False(tracker.TryBegin());

            tracker.Complete();
            await drainTask;
        }

        [Fact]
        public async Task DrainAsync_ShouldCompleteImmediatelyWithoutActiveHandlers()
        {
            var tracker = new InFlightMessageTracker();

            await tracker.DrainAsync(TimeSpan.FromSeconds(1));

            Assert.False(tracker.TryBegin());
        }
    }
}
