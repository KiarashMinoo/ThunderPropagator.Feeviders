using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using System.Net;
using System.Net.Sockets;
using ThunderPropagator.Feeviders.UdpClient.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.UdpClient
{
    internal
#if !DEBUG
        sealed
#endif
        class UdpClientProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration> : AbstractProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>
        where TUdpClientProviderMessage : UdpClientProviderMessage
        where TUdpClientProviderConfiguration : UdpClientProviderConfiguration
    {
        private readonly TUdpClientProviderConfiguration _udpClientProviderConfiguration;
        private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);
        private readonly IPEndPoint _remoteEndpoint;
        private readonly System.Net.Sockets.UdpClient _udpClient;

        private readonly UdpMessageProtector? _messageProtector;

        public UdpClientProvider(TUdpClientProviderConfiguration udpClientProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _udpClientProviderConfiguration = udpClientProviderConfiguration;
            _remoteEndpoint = new IPEndPoint(IPAddress.Parse(_udpClientProviderConfiguration.Endpoint), _udpClientProviderConfiguration.Port);
            _udpClient = new System.Net.Sockets.UdpClient();

            if (_udpClientProviderConfiguration.EnableEncryption && !string.IsNullOrEmpty(_udpClientProviderConfiguration.EncryptionKey))
                _messageProtector = new UdpMessageProtector(_udpClientProviderConfiguration.EncryptionKey);
        }

        protected override Task InternalExecuteAsync(TUdpClientProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            if (Activity.Current?.Context is not null)
                feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

            feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());

            return Task.CompletedTask;
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            await _semaphoreSlim.WaitAsync(cancellationToken);

            try
            {
                byte[] dataToSend = bytes;

                if (_messageProtector is not null)
                    dataToSend = _messageProtector.Protect(bytes);

                await _udpClient.SendAsync(dataToSend, dataToSend.Length, _remoteEndpoint);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while posting message to path {Endpoint}, port {Port}.",
                    _udpClientProviderConfiguration.Endpoint, _udpClientProviderConfiguration.Port);
                throw;
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        protected override void DisposeManagedResources()
        {
            _udpClient.Close();
            _udpClient.Dispose();
            _messageProtector?.Dispose();
        }
    }
}