using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using System.Net.WebSockets;

namespace ThunderPropagator.Providers.DotNet.WebSocket
{
    internal
#if !DEBUG
        sealed
#endif
        partial class WebSocketProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration> : AbstractProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration>
        where TWebSocketProviderMessage : WebSocketProviderMessage
        where TWebSocketProviderConfiguration : WebSocketProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4701, Level = LogLevel.Error, Message = "error has occured while posting message to path {Endpoint}.")]
            public static partial void PostException(ILogger logger, Exception exception, string endpoint);

            [LoggerMessage(EventId = 4702, Level = LogLevel.Warning, Message = "Exception while closing ClientWebSocket during dispose.")]
            public static partial void CloseWarning(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4703, Level = LogLevel.Warning, Message = "Exception while disposing ClientWebSocket.")]
            public static partial void DisposeWarning(ILogger logger, Exception exception);
        }

        private readonly TWebSocketProviderConfiguration _webSocketProviderConfiguration;
        private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);
        private ClientWebSocket _clientWebSocket;

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
                {
                    _clientWebSocket = ClientWebSocketFactory.GetConnectable(_clientWebSocket);
                    await _clientWebSocket.ConnectAsync(new Uri(_webSocketProviderConfiguration.Endpoint), cancellationToken).ConfigureAwait(false);
                }

                await _clientWebSocket.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.PostException(Logger, exception, _webSocketProviderConfiguration.Endpoint);
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
                Log.CloseWarning(Logger, ex);
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
                Log.DisposeWarning(Logger, ex);
            }
        }
    }
}