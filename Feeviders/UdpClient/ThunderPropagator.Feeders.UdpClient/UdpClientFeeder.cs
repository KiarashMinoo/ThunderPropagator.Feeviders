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
using System.Security.Cryptography;
using System.Text;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.Feeders.UdpClient
{
    internal
#if !DEBUG
        sealed
#endif
        class UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration> : DelegativeFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TUdpClientFeederMessage : UdpClientFeederMessage
        where TUdpClientFeederConfiguration : UdpClientFeederConfiguration
    {
        private readonly TUdpClientFeederConfiguration _udpClientFeederConfiguration;
        private readonly Socket _socket;
        private readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Shared;
        private readonly HashSet<string>? _allowedAddressesSet;
        private readonly InFlightMessageTracker _inFlightMessages = new();
        private readonly CancellationTokenSource _receiveCancellation = new();
        private Task _backgroundTask = Task.CompletedTask;

        // Encryption support
        private readonly Aes? _aes;
        private readonly HMACSHA256? _hmac;

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

            // Initialize encryption if enabled
            if (_udpClientFeederConfiguration.EnableEncryption && !string.IsNullOrEmpty(_udpClientFeederConfiguration.EncryptionKey))
            {
                _aes = Aes.Create();
                _aes.Key = Encoding.UTF8.GetBytes(_udpClientFeederConfiguration.EncryptionKey.PadRight(32).Substring(0, 32)); // Ensure 256-bit key
                _aes.Mode = CipherMode.CBC;
                _aes.Padding = PaddingMode.PKCS7;

                _hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_udpClientFeederConfiguration.EncryptionKey));
            }

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

                        Logger.LogInformation($"Received from {result.RemoteEndPoint}");

                        if (!CheckAllowance(result.RemoteEndPoint))
                            continue;

                        // Use span to avoid allocating new array for received bytes
                        var receivedSpan = buffer.AsSpan(0, result.ReceivedBytes);
                        byte[] messageBytes = ( _aes is not null && _hmac is not null ) ? DecryptMessage(receivedSpan.ToArray()) : receivedSpan.ToArray();

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

                        Logger.LogError(exception, "error has occured while consuming messages on port {Port}.", string.Join(',', _udpClientFeederConfiguration.Port));
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
                Logger.LogError(ex, "Unhandled exception in UDP feeder background loop.");
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

        private byte[] DecryptMessage(byte[] encryptedData)
        {
            if (_aes is null || _hmac is null)
                throw new InvalidOperationException("Encryption not properly initialized");

            try
            {
                // Extract IV (first 16 bytes), HMAC (next 32 bytes), and encrypted data
                var iv = encryptedData.AsSpan(0, 16).ToArray();
                var receivedHmac = encryptedData.AsSpan(16, 32).ToArray();
                var encryptedPayload = encryptedData.AsSpan(48).ToArray();

                // Verify HMAC
                var computedHmac = _hmac.ComputeHash(encryptedPayload);
                if (!CryptographicOperations.FixedTimeEquals(computedHmac, receivedHmac))
                    throw new CryptographicException("Message integrity check failed");

                // Decrypt
                using var decryptor = _aes.CreateDecryptor(_aes.Key, iv);
                return decryptor.TransformFinalBlock(encryptedPayload, 0, encryptedPayload.Length);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to decrypt UDP message");
                throw;
            }
        }

        protected override void DisposeManagedResources()
        {
            try
            {
                _socket?.Close();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while closing UDP socket.");
            }

            try
            {
                _socket?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing UDP socket.");
            }

            _receiveCancellation.Dispose();
            base.DisposeManagedResources();
        }
    }
}
