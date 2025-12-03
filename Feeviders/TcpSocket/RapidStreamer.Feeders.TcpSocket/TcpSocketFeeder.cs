using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.TcpSocket.SharedKernel;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Microsoft.Extensions.Hosting;
using RapidStreamer.Application;

namespace RapidStreamer.Feeders.TcpSocket
{
    internal
#if !DEBUG
        sealed
#endif
        class TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration> : DelegativeFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TTcpSocketFeederMessage : TcpSocketFeederMessage
        where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration
    {
        private class FramedStreamReader(Stream stream, ReadOnlySpan<byte> eom)
        {
            private readonly ReadOnlyMemory<byte> _eom = eom.ToArray();

            public async Task<byte[]> ReadUntilEomAsync(int bufferSize, CancellationToken cancellationToken = default)
            {
                // Use ArrayPool for buffer management to reduce allocations
                byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
                try
                {
                    // Pre-allocate with estimated capacity to reduce reallocations
                    int estimatedCapacity = Math.Min(bufferSize * 2, 8192);
                    using var memoryStream = new MemoryStream(estimatedCapacity);

                    while (true)
                    {
                        int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, bufferSize), cancellationToken);
                        if (bytesRead == 0) break;

                        memoryStream.Write(buffer.AsSpan(0, bytesRead));

                        // Check for EOM without additional allocations
                        if (EndsWithEom(memoryStream.GetBuffer().AsSpan(0, (int)memoryStream.Length)))
                        {
                            // Remove EOM from the end
                            memoryStream.SetLength(memoryStream.Length - _eom.Length);
                            break;
                        }
                    }

                    return memoryStream.ToArray();
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            private bool EndsWithEom(ReadOnlySpan<byte> data)
            {
                if (data.Length < _eom.Length) return false;
                return data[^_eom.Length..].SequenceEqual(_eom.Span);
            }
        }


        private readonly TTcpSocketFeederConfiguration _tcpSocketFeederConfiguration;
        private readonly IHostApplicationLifetime _applicationLifetime;
        private readonly TcpListener _listener;
        private readonly ReadOnlyMemory<byte> _eomBytes;
        private readonly ReadOnlyMemory<byte> _authenticationPrefixBytes;
        private readonly ReadOnlyMemory<byte> _usernamePrefixBytes;
        private readonly ReadOnlyMemory<byte> _passwordPrefixBytes;
        private readonly ReadOnlyMemory<byte> _separatorBytes;
        private readonly bool _requiresAuthentication;

        public TcpSocketFeeder(TChannel channel,
            TTcpSocketFeederConfiguration tcpSocketFeederConfiguration,
            IFeederHandler<TChannel, TTcpSocketFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, tcpSocketFeederConfiguration, feederHandler, serviceProvider)
        {
            _tcpSocketFeederConfiguration = tcpSocketFeederConfiguration;
            _applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            // Pre-compute byte arrays to avoid repeated allocations
            _eomBytes = Encoding.UTF8.GetBytes(Constants.Eom);
            _authenticationPrefixBytes = Encoding.UTF8.GetBytes(Constants.Authentication);
            _usernamePrefixBytes = Encoding.UTF8.GetBytes(Constants.Username);
            _passwordPrefixBytes = Encoding.UTF8.GetBytes(Constants.Password);
            _separatorBytes = Encoding.UTF8.GetBytes(Constants.Separator);

            _requiresAuthentication = !string.IsNullOrWhiteSpace(_tcpSocketFeederConfiguration.Username) &&
                                     !string.IsNullOrWhiteSpace(_tcpSocketFeederConfiguration.Password);

            HealthName = $"feeder_{nameof(TcpSocket)}_{tcpSocketFeederConfiguration.Port}";
            HealthTags = [.. HealthTags, nameof(TcpSocket), tcpSocketFeederConfiguration.Port.ToString()];

            _listener = new TcpListener(IPAddress.Any, tcpSocketFeederConfiguration.Port);
            _listener.Start();

            // Use Task.Run instead of new Thread for better async integration and observe top-level errors
            _ = Task.Run(() => StartAsync_CatchAll(), _applicationLifetime.ApplicationStopping);
        }

        private async Task StartAsync()
        {
            while (!IsStopped)
            {
                TcpClient? client = null;
                Stream? stream = null;

                try
                {
                    client = await _listener.AcceptTcpClientAsync(_applicationLifetime.ApplicationStopping).ConfigureAwait(false);

                    if (!CheckAllowance(client.Client.RemoteEndPoint))
                    {
                        client.Close();
                        continue;
                    }

                    stream = _tcpSocketFeederConfiguration.Ssl == true
                        ? new SslStream(client.GetStream(), false)
                        : client.GetStream();

                    // Configure stream timeouts
                    ConfigureStreamTimeouts(stream);

                    // Handle SSL authentication if required
                    if (stream is SslStream sslStream)
                    {
                        await sslStream.AuthenticateAsServerAsync(
                            _tcpSocketFeederConfiguration.Certificate?.Certificate ?? throw new ArgumentNullException(nameof(_tcpSocketFeederConfiguration.Certificate)),
                            _tcpSocketFeederConfiguration.ClientCertificateRequired,
                            _tcpSocketFeederConfiguration.EnabledSslProtocols,
                            _tcpSocketFeederConfiguration.CheckCertificateRevocation).ConfigureAwait(false);
                    }

                    // Read message data
                    var reader = new FramedStreamReader(stream, _eomBytes.Span);
                    var bytes = await reader.ReadUntilEomAsync(_tcpSocketFeederConfiguration.BufferSize, _applicationLifetime.ApplicationStopping).ConfigureAwait(false);

                    if (bytes.Length == 0)
                    {
                        Logger.LogWarning("Client disconnected before EOM.");
                        continue;
                    }

                    // Handle authentication if required
                    if (_requiresAuthentication && !Authenticate(bytes))
                    {
                        Logger.LogWarning("Authentication failed.");
                        continue;
                    }

                    // Process the message
                    var tcpSocketFeederMessage = Deserialize(bytes) ??
                                                 throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

                    var activityContext = tcpSocketFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                    var baggage = tcpSocketFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                    await ReceiveAsync(tcpSocketFeederMessage, activityContext, baggage).ConfigureAwait(false);

                    ReportHealth(HealthStatus.Healthy);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);
                    Logger.LogError(exception, "An error occurred while serving the TCP socket client on port: {Port}.", _tcpSocketFeederConfiguration.Port);
                }
                finally
                {
                    // Ensure proper cleanup
                    stream?.Close();
                    client?.Close();
                }
            }
        }

        private async Task StartAsync_CatchAll()
        {
            try
            {
                await StartAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ReportHealth(HealthStatus.Unhealthy, ex);
                Logger.LogError(ex, "Unhandled exception in TCP socket feeder background loop.");
            }
        }

        private void ConfigureStreamTimeouts(Stream stream)
        {
            switch (stream)
            {
                case SslStream sslStream:
                    sslStream.ReadTimeout = _tcpSocketFeederConfiguration.ReadTimeout ?? Timeout.Infinite;
                    sslStream.WriteTimeout = _tcpSocketFeederConfiguration.WriteTimeout ?? Timeout.Infinite;
                    break;
                case NetworkStream networkStream:
                    networkStream.ReadTimeout = _tcpSocketFeederConfiguration.ReadTimeout ?? Timeout.Infinite;
                    networkStream.WriteTimeout = _tcpSocketFeederConfiguration.WriteTimeout ?? Timeout.Infinite;
                    break;
            }
        }

        private bool CheckAllowance(EndPoint? endPoint)
        {
            if (_tcpSocketFeederConfiguration.AllowedAddresses is null ||
                _tcpSocketFeederConfiguration.AllowedAddresses.Length == 0)
                return true;

            return endPoint is IPEndPoint ipEndPoint &&
                   _tcpSocketFeederConfiguration.AllowedAddresses.Contains(ipEndPoint.Address.ToString());
        }

        private bool Authenticate(ReadOnlySpan<byte> bytes)
        {
            // Quick check for authentication prefix
            if (!bytes.StartsWith(_authenticationPrefixBytes.Span))
                return false;

            // Skip the authentication prefix
            var authData = bytes[_authenticationPrefixBytes.Length..];

            // Find separator positions efficiently
            int separatorIndex = authData.IndexOf(_separatorBytes.Span);
            if (separatorIndex == -1) return false;

            var usernamePart = authData[..separatorIndex];
            var passwordPart = authData[(separatorIndex + _separatorBytes.Length)..];

            // Validate username prefix
            if (!usernamePart.StartsWith(_usernamePrefixBytes.Span))
                return false;

            // Validate password prefix
            if (!passwordPart.StartsWith(_passwordPrefixBytes.Span))
                return false;

            // Extract credentials
            var username = usernamePart[_usernamePrefixBytes.Length..];
            var password = passwordPart[_passwordPrefixBytes.Length..];

            // Compare with configured credentials (null checks already done in constructor)
            return username.SequenceEqual(Encoding.UTF8.GetBytes(_tcpSocketFeederConfiguration.Username!)) &&
                   password.SequenceEqual(Encoding.UTF8.GetBytes(_tcpSocketFeederConfiguration.Password!));
        }

        protected override void DisposeManagedResources()
        {
            _listener.Stop();
            _listener.Dispose();
        }
    }
}