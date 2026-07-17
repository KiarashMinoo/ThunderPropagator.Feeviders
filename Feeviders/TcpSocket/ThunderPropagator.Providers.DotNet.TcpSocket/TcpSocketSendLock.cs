namespace ThunderPropagator.Providers.DotNet.TcpSocket
{
    internal readonly struct TcpSocketSendLock : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;

        private TcpSocketSendLock(SemaphoreSlim semaphore)
        {
            _semaphore = semaphore;
        }

        public static async ValueTask<TcpSocketSendLock> AcquireAsync(
            SemaphoreSlim semaphore,
            CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new TcpSocketSendLock(semaphore);
        }

        public void Dispose()
        {
            _semaphore.Release();
        }
    }
}
