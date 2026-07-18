using System.Net.WebSockets;
using ThunderPropagator.Providers.DotNet.WebSocket;

namespace ThunderPropagator.UnitTests
{
    public class ClientWebSocketFactoryTests
    {
        [Fact]
        public void GetConnectable_ShouldReplaceAbortedClient()
        {
            var abortedClient = new ClientWebSocket();
            abortedClient.Abort();

            using var connectableClient = ClientWebSocketFactory.GetConnectable(abortedClient);

            Assert.NotSame(abortedClient, connectableClient);
            Assert.Equal(WebSocketState.None, connectableClient.State);
        }

        [Fact]
        public void GetConnectable_ShouldReplaceClosedClient()
        {
            var closedClient = new ClientWebSocket();
            closedClient.Dispose();

            using var connectableClient = ClientWebSocketFactory.GetConnectable(closedClient);

            Assert.NotSame(closedClient, connectableClient);
            Assert.Equal(WebSocketState.None, connectableClient.State);
        }

        [Fact]
        public void GetConnectable_ShouldReuseNewClient()
        {
            using var newClient = new ClientWebSocket();

            var connectableClient = ClientWebSocketFactory.GetConnectable(newClient);

            Assert.Same(newClient, connectableClient);
        }
    }
}
