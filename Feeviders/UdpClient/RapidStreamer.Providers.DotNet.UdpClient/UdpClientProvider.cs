using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Net;

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

        public UdpClientProvider(TUdpClientProviderConfiguration udpClientProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _udpClientProviderConfiguration = udpClientProviderConfiguration;
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
                using var udpClient = new System.Net.Sockets.UdpClient();
                var remoteEndpoint = new IPEndPoint(IPAddress.Parse(_udpClientProviderConfiguration.Endpoint), _udpClientProviderConfiguration.Port);

                await udpClient.SendAsync(bytes, bytes.Length, remoteEndpoint);
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
    }
}