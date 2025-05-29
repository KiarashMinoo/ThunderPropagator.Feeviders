using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.TcpSocket.SharedKernel;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Microsoft.Extensions.Hosting;
using RapidStreamer.Application;
using RapidStreamer.Application.LicenseManagers.Providers.Demo;

namespace RapidStreamer.Feeders.TcpSocket
{
    [IsAvailableOnDemo]
    internal
#if !DEBUG
        sealed
#endif
        class TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration> : DelegativeFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TTcpSocketFeederMessage : TcpSocketFeederMessage
        where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration
    {
        private class FramedStreamReader(Stream stream, byte[] eom)
        {
            public async Task<byte[]> ReadUntilEomAsync(int bufferSize, CancellationToken cancellationToken = default)
            {
                var buffer = new byte[bufferSize];
                var bytes = new List<byte>();

                while (true)
                {
                    var bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken);
                    if (bytesRead == 0) break;

                    bytes.AddRange(buffer.AsSpan(0, bytesRead).ToArray());

                    if (EndsWithEom(bytes))
                    {
                        bytes.RemoveRange(bytes.Count - eom.Length, eom.Length);
                        break;
                    }
                }

                return bytes.ToArray();
            }

            private bool EndsWithEom(List<byte> data)
            {
                if (data.Count < eom.Length) return false;
                for (int i = 0; i < eom.Length; i++)
                {
                    if (data[data.Count - eom.Length + i] != eom[i])
                        return false;
                }

                return true;
            }
        }


        private readonly TTcpSocketFeederConfiguration _tcpSocketFeederConfiguration;
        private readonly IHostApplicationLifetime _applicationLifetime;
        private readonly TcpListener _listener;

        public TcpSocketFeeder(TChannel channel,
            TTcpSocketFeederConfiguration tcpSocketFeederConfiguration,
            IFeederHandler<TChannel, TTcpSocketFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, tcpSocketFeederConfiguration, feederHandler, serviceProvider)
        {
            _tcpSocketFeederConfiguration = tcpSocketFeederConfiguration;

            _applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            HealthName = $"feeder_{nameof(TcpSocket)}_{tcpSocketFeederConfiguration.Port.ToString()}";
            HealthTags = [.. HealthTags, nameof(TcpSocket), tcpSocketFeederConfiguration.Port.ToString()];

            _listener = new TcpListener(IPAddress.Any, tcpSocketFeederConfiguration.Port);
            _listener.Start();

            new Thread(Start).Start();
        }

        private async void Start(object? state)
        {
            var eom = Encoding.UTF8.GetBytes(Constants.Eom);
            var checkAuthentication = !string.IsNullOrWhiteSpace(_tcpSocketFeederConfiguration.Username) && !string.IsNullOrWhiteSpace(_tcpSocketFeederConfiguration.Username);

            while (!IsStopped)
            {
                using var client = await _listener.AcceptTcpClientAsync(_applicationLifetime.ApplicationStopping);

                if (!CheckAllowance(client.Client.RemoteEndPoint))
                {
                    client.Close();
                    continue;
                }

                await using Stream stream = _tcpSocketFeederConfiguration.Ssl == true
                    ? new SslStream(client.GetStream(), false)
                    : client.GetStream();

                switch (stream)
                {
                    case SslStream sslStream:
                    {
                        await sslStream.AuthenticateAsServerAsync(
                            _tcpSocketFeederConfiguration.Certificate?.Certificate ?? throw new ArgumentNullException(nameof(_tcpSocketFeederConfiguration.Certificate)),
                            _tcpSocketFeederConfiguration.ClientCertificateRequired,
                            _tcpSocketFeederConfiguration.EnabledSslProtocols,
                            _tcpSocketFeederConfiguration.CheckCertificateRevocation);

                        sslStream.ReadTimeout = _tcpSocketFeederConfiguration.ReadTimeout ?? Timeout.Infinite;
                        sslStream.WriteTimeout = _tcpSocketFeederConfiguration.WriteTimeout ?? Timeout.Infinite;
                        break;
                    }
                    case NetworkStream networkStream:
                    {
                        networkStream.ReadTimeout = _tcpSocketFeederConfiguration.ReadTimeout ?? Timeout.Infinite;
                        networkStream.WriteTimeout = _tcpSocketFeederConfiguration.WriteTimeout ?? Timeout.Infinite;
                        break;
                    }
                }

                try
                {
                    var reader = new FramedStreamReader(stream, eom);
                    var bytes = await reader.ReadUntilEomAsync(_tcpSocketFeederConfiguration.BufferSize, _applicationLifetime.ApplicationStopping);

                    if (bytes.Length == 0)
                    {
                        Logger.LogWarning("Client disconnected before EOM.");
                        client.Close();
                        stream.Close();
                        continue;
                    }

                    if (RequiresAuthentication() && !Authenticate(bytes))
                    {
                        Logger.LogWarning("Authentication failed.");
                        client.Close();
                        stream.Close();
                        continue;
                    }

                    var tcpSocketFeederMessage = Deserialize(bytes) ??
                                                 throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

                    var activityContext = tcpSocketFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                    var baggage = tcpSocketFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                    await ReceiveAsync(tcpSocketFeederMessage, activityContext, baggage);

                    ReportHealth(HealthStatus.Healthy);
                }
                catch (Exception exception)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Logger.LogError(exception, "An error occurred while serving the TCP socket client.port: {Port}.", _tcpSocketFeederConfiguration.Port);

                    throw;
                }
            }

            return;

            bool RequiresAuthentication()
                => !string.IsNullOrWhiteSpace(_tcpSocketFeederConfiguration.Username) && !string.IsNullOrWhiteSpace(_tcpSocketFeederConfiguration.Password);

            bool CheckAllowance(EndPoint? endPoint)
                => _tcpSocketFeederConfiguration.AllowedAddresses is null ||
                   _tcpSocketFeederConfiguration.AllowedAddresses.Length == 0 ||
                   endPoint is IPEndPoint ipEndPoint &&
                   _tcpSocketFeederConfiguration.AllowedAddresses.Contains(ipEndPoint.Address.ToString());

            bool Authenticate(byte[] bytes)
            {
                var authentication = Encoding.UTF8.GetString(bytes);
                if (!authentication.StartsWith(Constants.Authentication))
                {
                    Logger.LogError(nameof(InvalidCredentialException));
                    return false;
                }

                var authenticationParts = authentication
                    .Replace(Constants.Authentication, string.Empty)
                    .Split(Constants.Separator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                if (authenticationParts.Length != 2) return false;
                
                if (!authenticationParts[0].StartsWith(Constants.Username) || !authenticationParts[1].StartsWith(Constants.Password)) return false;

                var username = authenticationParts[0][Constants.Username.Length..];
                var password = authenticationParts[1][Constants.Password.Length..];

                return username == _tcpSocketFeederConfiguration.Username &&
                       password == _tcpSocketFeederConfiguration.Password;
            }
        }

        protected override void DisposeManagedResources()
        {
            _listener.Stop();
            _listener.Dispose();
        }
    }
}