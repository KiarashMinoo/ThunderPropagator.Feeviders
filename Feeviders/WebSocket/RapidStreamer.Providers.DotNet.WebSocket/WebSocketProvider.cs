using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Net.WebSockets;

namespace RapidStreamer.Providers.DotNet.WebSocket
{
    internal
#if !DEBUG
        sealed
#endif
        class WebSocketProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration> : AbstractProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration>
        where TWebSocketProviderMessage : WebSocketProviderMessage
        where TWebSocketProviderConfiguration : WebSocketProviderConfiguration
    {
        private readonly TWebSocketProviderConfiguration _webSocketProviderConfiguration;
        private readonly ClientWebSocket _clientWebSocket;
        private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);

        public WebSocketProvider(TWebSocketProviderConfiguration webSocketProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _webSocketProviderConfiguration = webSocketProviderConfiguration;
            _clientWebSocket = new ClientWebSocket();
        }

        protected override Task InternalExecuteAsync(TWebSocketProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            if (Activity.Current?.Context is not null)
                feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

            feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());

            return Task.CompletedTask;
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            await _semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                if (_clientWebSocket.State != WebSocketState.Open)
                    await _clientWebSocket.ConnectAsync(new Uri(_webSocketProviderConfiguration.Endpoint), cancellationToken).ConfigureAwait(false);

                await _clientWebSocket.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while posting message to path {Endpoint}.",
                    _webSocketProviderConfiguration.Endpoint);
                throw;
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            try
            {
                if (_clientWebSocket.State == WebSocketState.Open)
                    await _clientWebSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "shutting down", CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while closing ClientWebSocket during dispose.");
            }
        }

        protected override void DisposeManagedResources()
        {
            try
            {
                _clientWebSocket.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing ClientWebSocket.");
            }
        }
    }
}