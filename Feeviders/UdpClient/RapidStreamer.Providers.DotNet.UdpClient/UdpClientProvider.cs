using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RapidStreamer.Providers.DotNet.UdpClient
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
        private readonly Socket _socket;
        private readonly EndPoint _endPoint;
        private readonly byte[] _endMessageCode;
        private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);

        public UdpClientProvider(TUdpClientProviderConfiguration udpClientProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _udpClientProviderConfiguration = udpClientProviderConfiguration;
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _endPoint = new IPEndPoint(IPAddress.Parse(_udpClientProviderConfiguration.Endpoint), _udpClientProviderConfiguration.Port);
            _endMessageCode = Encoding.UTF8.GetBytes(_udpClientProviderConfiguration.EndMessageCode);
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

            if (!_socket.Connected)
                await _socket.ConnectAsync(_endPoint, cancellationToken);

            try
            {
                foreach (var buffer in bytes.Splice(_udpClientProviderConfiguration.BufferSize))
                    await _socket.SendAsync(buffer, cancellationToken);

                await _socket.SendAsync(_endMessageCode, cancellationToken);
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
            _socket.Close();
            _socket.Dispose();
        }
    }
}