using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Feeviders.TcpSocket.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Buffers;
using System.Text;

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
        private TcpClient _tcpClient;
        private readonly IPEndPoint _endPoint;
        private Stream? _stream;
        private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);

        // Pre-computed byte arrays for performance
        private readonly byte[] _eomBytes = Encoding.UTF8.GetBytes(Constants.Eom);
        private readonly byte[]? _authenticationBytes;
        private readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Shared;

        public TcpSocketProvider(TTcpSocketProviderConfiguration tcpSocketProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _tcpSocketProviderConfiguration = tcpSocketProviderConfiguration;
            _tcpClient = new TcpClient();
            _endPoint = new IPEndPoint(IPAddress.Parse(_tcpSocketProviderConfiguration.Endpoint), _tcpSocketProviderConfiguration.Port);

            // Pre-compute authentication bytes if credentials are provided
            if (!string.IsNullOrEmpty(_tcpSocketProviderConfiguration.Username) && !string.IsNullOrWhiteSpace(_tcpSocketProviderConfiguration.Password))
            {
                var authentication = $"{Constants.Authentication}{Constants.Username}{_tcpSocketProviderConfiguration.Username}{Constants.Separator}{Constants.Password}{_tcpSocketProviderConfiguration.Password}";
                _authenticationBytes = Encoding.UTF8.GetBytes(authentication);
            }
        }

        protected override Task InternalExecuteAsync(TTcpSocketProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            if (Activity.Current?.Context is not null)
                feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

            feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());

            return Task.CompletedTask;
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            await _semaphoreSlim.WaitAsync(cancellationToken);

            if (!IsSocketConnected())
            {
                _tcpClient.Close();
                _tcpClient.Dispose();
                _tcpClient = new TcpClient();
                await _tcpClient.ConnectAsync(_endPoint, cancellationToken);

                Logger.LogInformation("TCP client connected to {Endpoint}:{Port}", _endPoint.Address, _endPoint.Port);

                _stream = await InitializeStreamAsync();

                if (_authenticationBytes is not null)
                {
                    await _stream.WriteAsync(_authenticationBytes, cancellationToken);
                    await SendEomAsync();
                }
            }

            ArgumentNullException.ThrowIfNull(_stream);

            try
            {
                // Use pooled buffer for efficient chunking
                var bufferSize = _tcpSocketProviderConfiguration.BufferSize;
                for (int offset = 0; offset < bytes.Length; offset += bufferSize)
                {
                    var remaining = bytes.Length - offset;
                    var chunkSize = Math.Min(bufferSize, remaining);
                    var buffer = _bufferPool.Rent(chunkSize);
                    
                    try
                    {
                        bytes.AsSpan(offset, chunkSize).CopyTo(buffer);
                        await _stream.WriteAsync(buffer.AsMemory(0, chunkSize), cancellationToken);
                    }
                    finally
                    {
                        _bufferPool.Return(buffer);
                    }
                }

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

            bool IsSocketConnected()
            {
                try
                {
                    if (!_tcpClient.Connected) return false;

                    var socket = _tcpClient.Client;
                    return !(socket.Poll(1, SelectMode.SelectRead) && socket.Available == 0);
                }
                catch
                {
                    return false;
                }
            }

            async Task<Stream> InitializeStreamAsync()
            {
                if (_tcpSocketProviderConfiguration.Ssl == true)
                {
                    var sslStream = new SslStream(_tcpClient.GetStream());
                    await sslStream.AuthenticateAsClientAsync(_tcpSocketProviderConfiguration.Endpoint);
                    return sslStream;
                }

                return _tcpClient.GetStream();
            }

            async ValueTask SendEomAsync()
            {
                await _stream.WriteAsync(_eomBytes, cancellationToken);
                await _stream.FlushAsync(cancellationToken);
            }
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