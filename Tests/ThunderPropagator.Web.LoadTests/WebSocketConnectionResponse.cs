using System.Net.WebSockets;

namespace ThunderPropagator.Web.LoadTests
{
    public class WebSocketConnectionResponse
    {
        public virtual WebSocket WebSocket { get; set; } = null!;

        public DateTime StartConnectingDateTime { get; set; }
        public DateTime EndConnectingDateTime { get; set; }

        public long DiffConnectingDateTime => (EndConnectingDateTime - StartConnectingDateTime).Ticks;

        public Exception? Error { get; set; }

        public WebSocketState State => WebSocket.State;
    }
}