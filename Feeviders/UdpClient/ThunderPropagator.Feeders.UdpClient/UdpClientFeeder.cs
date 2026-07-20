using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application;
using System.Buffers;
using System.Collections.Generic;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeviders.UdpClient.SharedKernel;

namespace ThunderPropagator.Feeders.UdpClient
{
    internal
#if !DEBUG
        sealed
#endif
        partial class UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration> : DelegativeFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TUdpClientFeederMessage : UdpClientFeederMessage
        where TUdpClientFeederConfiguration : UdpClientFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 5000, Level = LogLevel.Information, Message = "Received from {RemoteEndPoint}")]
            public static partial void ReceivedFrom(ILogger logger, EndPoint remoteEndPoint);

            [LoggerMessage(EventId = 5001, Level = LogLevel.Error, Message = "error has occured while consuming messages on port {Port}.")]
            public static partial void ConsumeError(ILogger logger, Exception exception, string port);

            [LoggerMessage(EventId = 5002, Level = LogLevel.Error, Message = "Unhandled exception in UDP feeder background loop.")]
            public static partial void UnhandledBackgroundLoopException(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 5003, Level = LogLevel.Warning, Message = "Exception while closing UDP socket.")]
            public static partial void SocketCloseException(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 5004, Level = LogLevel.Warning, Message = "Exception while disposing UDP socket.")]
            public static partial void SocketDisposeException(ILogger logger, Exception exception);
        }

        private readonly TUdpClientFeederConfiguration _udpClientFeederConfiguration;
        private readonly Socket _socket;
        private readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Shared;
        private readonly HashSet<string>? _allowedAddressesSet;
        private readonly InFlightMessageTracker _inFlightMessages = new();
        private readonly CancellationTokenSource _receiveCancellation = new();
        private Task _backgroundTask = Task.CompletedTask;

        private readonly UdpMessageProtector? _messageProtector;

        public UdpClientFeeder(TChannel channel,
            TUdpClientFeederConfiguration udpClientFeederConfiguration,
            IFeederHandler<TChannel, TUdpClientFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, udpClientFeederConfiguration, feederHandler, serviceProvider)
        {
            _udpClientFeederConfiguration = udpClientFeederConfiguration;

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

            // Pre-compute allowed addresses set for efficient lookups
            _allowedAddressesSet = _udpClientFeederConfiguration.AllowedAddresses is not null && _udpClientFeederConfiguration.AllowedAddresses.Length > 0
                ? new HashSet<string>(_udpClientFeederConfiguration.AllowedAddresses)
                : null;

            if (_udpClientFeederConfiguration.EnableEncryption && !string.IsNullOrEmpty(_udpClientFeederConfiguration.EncryptionKey))
                _messageProtector = new UdpMessageProtector(_udpClientFeederConfiguration.EncryptionKey);

            HealthName = $"feeder_{nameof(UdpClient)}_{udpClientFeederConfiguration.Port.ToString()}";
            HealthTags = [.. HealthTags, nameof(UdpClient), udpClientFeederConfiguration.Port.ToString()];
        }

        protected override Task StartAsync(CancellationToken cancellationToken = default)
        {
            _socket.Bind(new IPEndPoint(IPAddress.Any, _udpClientFeederConfiguration.Port));
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
            var buffer = _bufferPool.Rent(_udpClientFeederConfiguration.BufferSize);

            try
            {
                while (!IsStopped && !_receiveCancellation.IsCancellationRequested)
                {
                    try
                    {
                        EndPoint remoteEndpoint = new IPEndPoint(IPAddress.Any, 0);
                        var result = await _socket.ReceiveFromAsync(
                            buffer.AsMemory(0, _udpClientFeederConfiguration.BufferSize),
                            SocketFlags.None,
                            remoteEndpoint,
                            _receiveCancellation.Token).ConfigureAwait(false);

                        Log.ReceivedFrom(Logger, result.RemoteEndPoint);

                        if (!CheckAllowance(result.RemoteEndPoint))
                            continue;

                        // Use span to avoid allocating new array for received bytes
                        var receivedSpan = buffer.AsSpan(0, result.ReceivedBytes);
                        byte[] messageBytes = _messageProtector is not null ? _messageProtector.Unprotect(receivedSpan.ToArray()) : receivedSpan.ToArray();

                        if (!_inFlightMessages.TryBegin())
                            continue;

                        try
                        {
                            var udpClientFeederMessage = Deserialize(messageBytes) ?? throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

                            var activityContext = udpClientFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                            var baggage = udpClientFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                            await ReceiveAsync(udpClientFeederMessage, activityContext, baggage, cancellationToken: _receiveCancellation.Token).ConfigureAwait(false);

                            ReportHealth(HealthStatus.Healthy);
                        }
                        finally
                        {
                            _inFlightMessages.Complete();
                        }
                    }
                    catch (Exception) when (IsStopped || _receiveCancellation.IsCancellationRequested)
                    {
                        // Expected when shutdown closes the socket or cancels a receive.
                    }
                    catch (Exception exception)
                    {
                        ReportHealth(HealthStatus.Unhealthy, exception);

                        Log.ConsumeError(Logger, exception, string.Join(',', _udpClientFeederConfiguration.Port));
                    }
                }
            }
            finally
            {
                _bufferPool.Return(buffer);
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
                Log.UnhandledBackgroundLoopException(Logger, ex);
                ReportHealth(HealthStatus.Unhealthy, ex);
            }
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            var drainTask = _inFlightMessages.DrainAsync(TimeSpan.FromSeconds(5), cancellationToken);
            await drainTask.ConfigureAwait(false);
            await _receiveCancellation.CancelAsync().ConfigureAwait(false);
            await _backgroundTask.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        private bool CheckAllowance(EndPoint? endPoint)
            => _allowedAddressesSet is null || endPoint is IPEndPoint ipEndPoint && _allowedAddressesSet.Contains(ipEndPoint.Address.ToString());

        protected override void DisposeManagedResources()
        {
            try
            {
                _socket?.Close();
            }
            catch (Exception ex)
            {
                Log.SocketCloseException(Logger, ex);
            }

            try
            {
                _socket?.Dispose();
            }
            catch (Exception ex)
            {
                Log.SocketDisposeException(Logger, ex);
            }

            _receiveCancellation.Dispose();
            _messageProtector?.Dispose();
            base.DisposeManagedResources();
        }
    }
}
