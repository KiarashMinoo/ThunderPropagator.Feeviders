using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.TcpSocket.SharedKernel;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Features;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.Feeders.TcpSocket
{
    internal
#if !DEBUG
        sealed
#endif
        partial class TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration> : DelegativeFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TTcpSocketFeederMessage : TcpSocketFeederMessage
        where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4900, Level = LogLevel.Warning, Message = "Client disconnected before EOM.")]
            public static partial void ClientDisconnectedBeforeEom(ILogger logger);

            [LoggerMessage(EventId = 4901, Level = LogLevel.Warning, Message = "Authentication failed.")]
            public static partial void AuthenticationFailed(ILogger logger);

            [LoggerMessage(EventId = 4902, Level = LogLevel.Error, Message = "An error occurred while serving the TCP socket client on port: {Port}.")]
            public static partial void ServeClientError(ILogger logger, Exception exception, short port);

            [LoggerMessage(EventId = 4903, Level = LogLevel.Error, Message = "Unhandled exception in TCP socket feeder background loop.")]
            public static partial void BackgroundLoopUnhandledException(ILogger logger, Exception exception);
        }

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
        private readonly TcpListener _listener;
        private readonly InFlightMessageTracker _inFlightMessages = new();
        private readonly CancellationTokenSource _receiveCancellation = new();
        private Task _backgroundTask = Task.CompletedTask;
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
        }

        protected override Task StartAsync(CancellationToken cancellationToken = default)
        {
            _listener.Start();
            _backgroundTask = Task.Factory
                .StartNew(StartAsync_CatchAll,
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)
                .Unwrap();

            return Task.CompletedTask;
        }

        private async Task RunAsync()
        {
            while (!IsStopped && !_receiveCancellation.IsCancellationRequested)
            {
                TcpClient? client = null;
                Stream? stream = null;

                try
                {
                    client = await _listener.AcceptTcpClientAsync(_receiveCancellation.Token).ConfigureAwait(false);

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
                    var receiveStopwatch = Stopwatch.StartNew();
                    Activity? receiveActivity = null;
                    try
                    {
                        var reader = new FramedStreamReader(stream, _eomBytes.Span);
                        var bytes = await reader.ReadUntilEomAsync(_tcpSocketFeederConfiguration.BufferSize, _receiveCancellation.Token).ConfigureAwait(false);

                        if (bytes.Length == 0)
                        {
                            Log.ClientDisconnectedBeforeEom(Logger);
                            continue;
                        }

                        // Handle authentication if required
                        if (_requiresAuthentication && !Authenticate(bytes))
                        {
                            Log.AuthenticationFailed(Logger);
                            continue;
                        }

                        // Process the message
                        if (!_inFlightMessages.TryBegin())
                            continue;

                        try
                        {
                            var tcpSocketFeederMessage = Deserialize(bytes) ??
                                                         throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

                            var activityContext = tcpSocketFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                            var baggage = tcpSocketFeederMessage[nameof(Baggage)] is Baggage b ? b : default;

                            receiveActivity = activityContext != default
                                ? TcpSocketTelemetry.ActivitySource.StartActivity("tcpsocket receive", ActivityKind.Consumer, activityContext)
                                : TcpSocketTelemetry.ActivitySource.StartActivity("tcpsocket receive", ActivityKind.Consumer);
                            receiveActivity?.SetTag("messaging.system", "tcpsocket");
                            receiveActivity?.SetTag("messaging.destination.name",
                                client.Client.RemoteEndPoint?.ToString() ?? $"0.0.0.0:{_tcpSocketFeederConfiguration.Port}");
                            receiveActivity?.SetTag("messaging.operation", "receive");

                            await ReceiveAsync(tcpSocketFeederMessage, activityContext, baggage, cancellationToken: _receiveCancellation.Token).ConfigureAwait(false);

                            ReportHealth(HealthStatus.Healthy);
                            TcpSocketTelemetry.MessagesReceived.Add(1);
                        }
                        finally
                        {
                            _inFlightMessages.Complete();
                        }
                    }
                    catch (Exception ex)
                    {
                        receiveActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                        TcpSocketTelemetry.MessagesReceiveFailed.Add(1);
                        throw;
                    }
                    finally
                    {
                        TcpSocketTelemetry.ReceiveDuration.Record(receiveStopwatch.Elapsed.TotalMilliseconds);
                        receiveActivity?.Dispose();
                    }
                }
                catch (Exception) when (IsStopped || _receiveCancellation.IsCancellationRequested)
                {
                    // Expected when shutdown stops the listener or cancels an active read.
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);
                    Log.ServeClientError(Logger, exception, _tcpSocketFeederConfiguration.Port);
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
                await RunAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ReportHealth(HealthStatus.Unhealthy, ex);
                Log.BackgroundLoopUnhandledException(Logger, ex);
            }
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            var drainTask = _inFlightMessages.DrainAsync(TimeSpan.FromSeconds(5), cancellationToken);
            _listener.Stop();
            await drainTask.ConfigureAwait(false);
            await _receiveCancellation.CancelAsync().ConfigureAwait(false);
            await _backgroundTask.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
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
            _receiveCancellation.Dispose();
            base.DisposeManagedResources();
        }
    }
}
