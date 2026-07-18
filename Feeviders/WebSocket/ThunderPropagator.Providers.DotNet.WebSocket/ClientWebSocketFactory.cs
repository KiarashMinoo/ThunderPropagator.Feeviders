using System.Net.WebSockets;

namespace ThunderPropagator.Providers.DotNet.WebSocket
{
    internal static class ClientWebSocketFactory
    {
        public static ClientWebSocket GetConnectable(ClientWebSocket clientWebSocket)
        {
            ArgumentNullException.ThrowIfNull(clientWebSocket);

            if (clientWebSocket.State is WebSocketState.None or WebSocketState.Open)
                return clientWebSocket;

            clientWebSocket.Dispose();
            return new ClientWebSocket();
        }
    }
}
