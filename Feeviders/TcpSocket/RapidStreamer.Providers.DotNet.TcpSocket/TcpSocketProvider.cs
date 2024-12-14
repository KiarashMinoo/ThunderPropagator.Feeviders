#if DEBUG
using OpenTelemetry;
using System.Diagnostics;
#endif
using Microsoft.Extensions.Logging;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Feeviders.TcpSocket.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;

namespace RapidStreamer.Providers.DotNet.TcpSocket
{
    internal
#if !DEBUG
        sealed
#endif
        class TcpSocketProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration> : AbstractProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>
        where TTcpSocketProviderMessage : TcpSocketProviderMessage
        where TTcpSocketProviderConfiguration : TcpSocketProviderConfiguration
    {
        private readonly TTcpSocketProviderConfiguration _tcpSocketProviderConfiguration;
        private readonly TcpClient _tcpClient;
        private readonly IPEndPoint _endPoint;
        private Stream? _stream;
        private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);

        public TcpSocketProvider(TTcpSocketProviderConfiguration tcpSocketProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _tcpSocketProviderConfiguration = tcpSocketProviderConfiguration;
            _tcpClient = new TcpClient();
            _endPoint = new IPEndPoint(IPAddress.Parse(_tcpSocketProviderConfiguration.Endpoint), _tcpSocketProviderConfiguration.Port);
        }

        protected override Task InternalExecuteAsync(TTcpSocketProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
#if DEBUG
            if (Activity.Current?.Context is not null)
                feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

            feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());
#endif
            return Task.CompletedTask;
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            await _semaphoreSlim.WaitAsync(cancellationToken);

            if (!_tcpClient.Connected)
            {
                await _tcpClient.ConnectAsync(_endPoint, cancellationToken);

                _stream = _tcpSocketProviderConfiguration.Ssl == true ? new SslStream(_tcpClient.GetStream()) : _tcpClient.GetStream();

                if (!string.IsNullOrEmpty(_tcpSocketProviderConfiguration.Username) && !string.IsNullOrWhiteSpace(_tcpSocketProviderConfiguration.Password))
                {
                    var authentication = $"{Constants.Authentication}{Constants.Username}{_tcpSocketProviderConfiguration.Username}{Constants.Separator}{Constants.Password}{_tcpSocketProviderConfiguration.Password}";
                    await _stream.WriteAsync(authentication.ToByteArray(), cancellationToken);
                    await SendEomAsync();
                }
            }

            ArgumentNullException.ThrowIfNull(_stream);

            try
            {
                foreach (var buffer in bytes.Splice(_tcpSocketProviderConfiguration.BufferSize))
                    await _stream.WriteAsync(buffer, cancellationToken);

                await SendEomAsync();
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while posting message to path {Endpoint}, port {Port}.",
                    _tcpSocketProviderConfiguration.Endpoint, _tcpSocketProviderConfiguration.Port);
                throw;
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            return;

            ValueTask SendEomAsync() => _stream.WriteAsync(Constants.Eom.ToByteArray(), cancellationToken);
        }

        protected override void DisposeManagedResources()
        {
            _stream?.Close();
            _stream?.Dispose();

            _tcpClient.Close();
            _tcpClient.Dispose();
        }
    }
}