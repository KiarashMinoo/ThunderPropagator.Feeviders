using RapidStreamer.BuildingBlocks.Application.Collections;
using System.Net.WebSockets;
using System.Text;

namespace RapidStreamer.Web.LoadTests
{
    public
#if !DEBUG
        sealed
#endif
        class WebSocketInteractiveResponse : WebSocketConnectionResponse
    {
        public override WebSocket WebSocket
        {
            get => base.WebSocket;
            set
            {
                base.WebSocket = value;
                HandleMessages();
            }
        }

        public string RequestId { get; set; } = null!;
        public BindingDictionary<DateTime, string> ReceivedMessage { get; } = new(true);

        public TimeSpan Duration => ReceivedMessage.Keys.Last() - ReceivedMessage.Keys.First();

        private void HandleMessages()
        {
            Task.Run(async () =>
            {
                while (base.WebSocket.State == WebSocketState.Open)
                {
                    var buffer = new byte[1024 * 8];

                    var result = await base.WebSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);

                    if (result.MessageType == WebSocketMessageType.Close)
                        return;

                    var message = Encoding.UTF8.GetString(buffer, 0, buffer.Length).Trim();
                    if (!message.StartsWith($"{{"))
                        ReceivedMessage.TryAdd<DateTime, string>(DateTime.UtcNow, message);
                }
            });
        }
    }
}