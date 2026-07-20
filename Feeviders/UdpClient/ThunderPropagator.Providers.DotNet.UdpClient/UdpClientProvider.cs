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
        partial class UdpClientProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration> : AbstractProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>
        where TUdpClientProviderMessage : UdpClientProviderMessage
        where TUdpClientProviderConfiguration : UdpClientProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 5005, Level = LogLevel.Error, Message = "error has occured while posting message to path {Endpoint}, port {Port}.")]
            public static partial void ProduceError(ILogger logger, Exception exception, string endpoint, short port);
        }

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
            using var activity = UdpClientTelemetry.ActivitySource.StartActivity("udpclient publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "udpclient");
            activity?.SetTag("messaging.destination.name", _remoteEndpoint.ToString());
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();

            await _semaphoreSlim.WaitAsync(cancellationToken);

            try
            {
                byte[] dataToSend = bytes;

                if (_messageProtector is not null)
                    dataToSend = _messageProtector.Protect(bytes);

                await _udpClient.SendAsync(dataToSend, dataToSend.Length, _remoteEndpoint);

                UdpClientTelemetry.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                Log.ProduceError(Logger, exception,
                    _udpClientProviderConfiguration.Endpoint, _udpClientProviderConfiguration.Port);

                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                UdpClientTelemetry.MessagesPublishFailed.Add(1);

                throw;
            }
            finally
            {
                _semaphoreSlim.Release();
                stopwatch.Stop();
                UdpClientTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
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