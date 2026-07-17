using ThunderPropagator.Providers.DotNet.TcpSocket;

namespace ThunderPropagator.UnitTests
{
    public class TcpSocketSendLockTests
    {
        [Fact]
        public async Task AcquireAsync_ShouldReleaseSemaphoreWhenSendThrows()
        {
            using var semaphore = new SemaphoreSlim(1, 1);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                using var sendLock = await TcpSocketSendLock.AcquireAsync(semaphore, CancellationToken.None);
                throw new InvalidOperationException("send failed");
            });

            Assert.True(await semaphore.WaitAsync(TimeSpan.FromSeconds(1)));
            semaphore.Release();
        }

        [Fact]
        public async Task AcquireAsync_ShouldNotReleaseWhenWaitIsCancelled()
        {
            using var semaphore = new SemaphoreSlim(0, 1);
            using var cancellation = new CancellationTokenSource();
            await cancellation.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await TcpSocketSendLock.AcquireAsync(semaphore, cancellation.Token));

            Assert.Equal(0, semaphore.CurrentCount);
        }
    }
}
