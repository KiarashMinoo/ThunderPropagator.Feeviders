using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RapidStreamer.Application;
using System.Buffers;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace RapidStreamer.Feeders.UdpClient
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
        private readonly IHostApplicationLifetime _applicationLifetime;
        private readonly Socket _socket;
        private readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Shared;
        private readonly HashSet<string>? _allowedAddressesSet;

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

            _applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.Bind(new IPEndPoint(IPAddress.Any, udpClientFeederConfiguration.Port));

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

            new Thread(Start).Start();
        }

        private async void Start(object? state)
        {
            var buffer = _bufferPool.Rent(_udpClientFeederConfiguration.BufferSize);

            try
            {
                while (!IsStopped)
                {
                    try
                    {
                        EndPoint remoteEndpoint = new IPEndPoint(IPAddress.Any, 0);
                        var result = await _socket.ReceiveFromAsync(buffer, SocketFlags.None, remoteEndpoint);

                        Logger.LogInformation($"Received from {result.RemoteEndPoint}");

                        if (!CheckAllowance(result.RemoteEndPoint))
                            continue;

                        // Use span to avoid allocating new array for received bytes
                        var receivedSpan = buffer.AsSpan(0, result.ReceivedBytes);
                        byte[] messageBytes;

                        if (_aes is not null && _hmac is not null)
                        {
                            // Decrypt the message
                            messageBytes = DecryptMessage(receivedSpan.ToArray());
                        }
                        else
                        {
                            messageBytes = receivedSpan.ToArray();
                        }

                        var udpClientFeederMessage = Deserialize(messageBytes) ??
                                                     throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

                        var activityContext = udpClientFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                        var baggage = udpClientFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                        await ReceiveAsync(udpClientFeederMessage, activityContext, baggage);

                        ReportHealth(HealthStatus.Healthy);
                    }
                    catch (Exception exception)
                    {
                        ReportHealth(HealthStatus.Unhealthy, exception);

                        Logger.LogError(exception,
                            "error has occured while consuming messages on port {Port}.",
                            string.Join(',', _udpClientFeederConfiguration.Port));
                    }
                }
            }
            finally
            {
                _bufferPool.Return(buffer);
            }

            return;

            bool CheckAllowance(EndPoint? endPoint)
                => _allowedAddressesSet is null ||
                   endPoint is IPEndPoint ipEndPoint && _allowedAddressesSet.Contains(ipEndPoint.Address.ToString());

            byte[] DecryptMessage(byte[] encryptedData)
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
        }

        protected override void DisposeManagedResources()
        {
            _socket.Close();
            _socket.Dispose();
        }
    }
}