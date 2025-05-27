using OpenTelemetry;
using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RapidStreamer.Application;

namespace RapidStreamer.Feeders.UdpClient
{
    internal
#if !DEBUG
        sealed
#endif
        class UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration> : DelegativeFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TUdpClientFeederMessage : UdpClientFeederMessage
        where TUdpClientFeederConfiguration : UdpClientFeederConfiguration
    {
        private readonly TUdpClientFeederConfiguration _udpClientFeederConfiguration;
        private readonly IHostApplicationLifetime _applicationLifetime;
        private readonly Socket _socket;

        public UdpClientFeeder(TChannel channel,
            TUdpClientFeederConfiguration udpClientFeederConfiguration,
            IFeederHandler<TChannel, TUdpClientFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, udpClientFeederConfiguration, feederHandler, serviceProvider)
        {
            _udpClientFeederConfiguration = udpClientFeederConfiguration;

            _applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.Bind(new IPEndPoint(IPAddress.Any, udpClientFeederConfiguration.Port));

            HealthName = $"feeder_{nameof(UdpClient)}_{udpClientFeederConfiguration.Port.ToString()}";
            HealthTags = [.. HealthTags, nameof(UdpClient), udpClientFeederConfiguration.Port.ToString()];

            new Thread(Start).Start();
        }

        private async void Start(object? state)
        {
            var buffer = new byte[_udpClientFeederConfiguration.BufferSize];

            while (!IsStopped)
            {
                try
                {
                    EndPoint remoteEndpoint = new IPEndPoint(IPAddress.Any, 0);
                    var result = await _socket.ReceiveFromAsync(buffer, SocketFlags.None, remoteEndpoint);

                    Logger.LogInformation($"Received from {result.RemoteEndPoint}");

                    if (!CheckAllowance(result.RemoteEndPoint))
                        continue;

                    var receivedBytes = buffer.AsSpan(0, result.ReceivedBytes).ToArray();

                    var udpClientFeederMessage = Deserialize(receivedBytes) ??
                                                 throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

                    var activityContext = udpClientFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                    var baggage = udpClientFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                    await ReceiveAsync(udpClientFeederMessage, activityContext, baggage);

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