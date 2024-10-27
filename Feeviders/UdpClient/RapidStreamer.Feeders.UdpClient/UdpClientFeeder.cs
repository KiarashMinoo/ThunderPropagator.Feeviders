#if DEBUG
using OpenTelemetry;
using System.Diagnostics;
#endif
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RapidStreamer.Feeders.UdpClient
{
    internal
#if !DEBUG
        sealed
#endif
        class UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration> : DelegativeFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>
        where TChannel : class, IChannel
        where TUdpClientFeederMessage : UdpClientFeederMessage
        where TUdpClientFeederConfiguration : UdpClientFeederConfiguration
    {
        private readonly TUdpClientFeederConfiguration _udpClientFeederConfiguration;
        private readonly Socket _socket;

        public UdpClientFeeder(TChannel channel,
            TUdpClientFeederConfiguration udpClientFeederConfiguration,
            IFeederHandler<TChannel, TUdpClientFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, udpClientFeederConfiguration, feederHandler, serviceProvider)
        {
            _udpClientFeederConfiguration = udpClientFeederConfiguration;

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.Bind(new IPEndPoint(IPAddress.Any, udpClientFeederConfiguration.Port));
            _socket.Listen();

            HealthName = $"feeder_{nameof(UdpClient)}_{udpClientFeederConfiguration.Port.ToString()}";
            HealthTags = [.. HealthTags, nameof(UdpClient), udpClientFeederConfiguration.Port.ToString()];

            new Thread(Start).Start();
        }

        private async void Start(object? state)
        {
            var endMessageCode = Encoding.UTF8.GetBytes(_udpClientFeederConfiguration.EndMessageCode);

            using var acceptedSocket = await _socket.AcceptAsync();

            if (!CheckAllowance(acceptedSocket.RemoteEndPoint))
                await acceptedSocket.DisconnectAsync(true);

            var buffer = new byte[_udpClientFeederConfiguration.BufferSize];

            while (!IsStopped)
            {
                try
                {
                    List<byte> bytes = [];
                    bool finished;
                    do
                    {
                        var bytesRead = await acceptedSocket.ReceiveAsync(buffer);
                        finished = bytesRead > 0 && buffer.Length == endMessageCode.Length && buffer.SequenceEqual(endMessageCode);
                        if (!finished)
                            bytes.AddRange(buffer);
                    } while (!finished);

                    var udpClientFeederMessage = Deserialize(bytes.ToArray()) ??
                                                 throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

#if DEBUG
                    var activityContext = udpClientFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                    var baggage = udpClientFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                    await ReceiveAsync(udpClientFeederMessage, activityContext, baggage);
#else
                    await ReceiveAsync(udpClientFeederMessage);
#endif

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

            acceptedSocket.Close();

            return;

            bool CheckAllowance(EndPoint? endPoint)
                => _udpClientFeederConfiguration.AllowedAddresses is null ||
                   _udpClientFeederConfiguration.AllowedAddresses.Length == 0 ||
                   endPoint is IPEndPoint ipEndPoint && _udpClientFeederConfiguration.AllowedAddresses.Contains(ipEndPoint.Address.ToString());
        }

        protected override void DisposeManagedResources()
        {
            _socket.Close();
            _socket.Dispose();
        }
    }
}