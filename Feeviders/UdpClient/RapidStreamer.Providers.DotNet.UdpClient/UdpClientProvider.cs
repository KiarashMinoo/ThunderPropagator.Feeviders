using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
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
        private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);
        private readonly IPEndPoint _remoteEndpoint;
        private readonly System.Net.Sockets.UdpClient _udpClient;

        // Encryption support
        private readonly Aes? _aes;
        private readonly HMACSHA256? _hmac;

        public UdpClientProvider(TUdpClientProviderConfiguration udpClientProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _udpClientProviderConfiguration = udpClientProviderConfiguration;
            _remoteEndpoint = new IPEndPoint(IPAddress.Parse(_udpClientProviderConfiguration.Endpoint), _udpClientProviderConfiguration.Port);
            _udpClient = new System.Net.Sockets.UdpClient();

            // Initialize encryption if enabled
            if (_udpClientProviderConfiguration.EnableEncryption && !string.IsNullOrEmpty(_udpClientProviderConfiguration.EncryptionKey))
            {
                _aes = Aes.Create();
                _aes.Key = Encoding.UTF8.GetBytes(_udpClientProviderConfiguration.EncryptionKey.PadRight(32).Substring(0, 32)); // Ensure 256-bit key
                _aes.Mode = CipherMode.CBC;
                _aes.Padding = PaddingMode.PKCS7;

                _hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_udpClientProviderConfiguration.EncryptionKey));
            }
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

                if (_aes is not null && _hmac is not null)
                {
                    // Encrypt the message
                    dataToSend = EncryptMessage(bytes);
                }

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

        private byte[] EncryptMessage(byte[] plainData)
        {
            if (_aes is null || _hmac is null)
                throw new InvalidOperationException("Encryption not properly initialized");

            try
            {
                _aes.GenerateIV();
                var iv = _aes.IV;

                // Encrypt the data
                using var encryptor = _aes.CreateEncryptor(_aes.Key, iv);
                var encryptedData = encryptor.TransformFinalBlock(plainData, 0, plainData.Length);

                // Compute HMAC for integrity
                var hmac = _hmac.ComputeHash(encryptedData);

                // Combine IV + HMAC + encrypted data
                var result = new byte[iv.Length + hmac.Length + encryptedData.Length];
                Buffer.BlockCopy(iv, 0, result, 0, iv.Length);
                Buffer.BlockCopy(hmac, 0, result, iv.Length, hmac.Length);
                Buffer.BlockCopy(encryptedData, 0, result, iv.Length + hmac.Length, encryptedData.Length);

                return result;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to encrypt UDP message");
                throw;
            }
        }

        protected override void DisposeManagedResources()
        {
            _udpClient.Close();
            _udpClient.Dispose();
        }
    }
}