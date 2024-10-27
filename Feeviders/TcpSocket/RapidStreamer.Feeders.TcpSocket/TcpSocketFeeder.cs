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
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace RapidStreamer.Feeders.TcpSocket
{
    internal
#if !DEBUG
        sealed
#endif
        class TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration> : DelegativeFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
        where TChannel : class, IChannel
        where TTcpSocketFeederMessage : TcpSocketFeederMessage
        where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration
    {
        private readonly TTcpSocketFeederConfiguration _tcpSocketFeederConfiguration;
        private readonly ILogger _logger;
        private readonly TcpListener _listener;

        public TcpSocketFeeder(TChannel channel,
            TTcpSocketFeederConfiguration tcpSocketFeederConfiguration,
            IFeederHandler<TChannel, TTcpSocketFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, tcpSocketFeederConfiguration, feederHandler, serviceProvider)
        {
            _tcpSocketFeederConfiguration = tcpSocketFeederConfiguration;
            _logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger(GetType());

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
                using var client = await _listener.AcceptTcpClientAsync();

                if (!CheckAllowance(client.Client.RemoteEndPoint))
                {
                    client.Close();
                    continue;
                }

                await using Stream stream = _tcpSocketFeederConfiguration.Ssl == true ? new SslStream(client.GetStream(), false) : client.GetStream();

                if (stream is SslStream sslStream)
                {
                    ArgumentException.ThrowIfNullOrWhiteSpace(_tcpSocketFeederConfiguration.CertFile);
                    var serverCertificate = X509Certificate.CreateFromCertFile(_tcpSocketFeederConfiguration.CertFile);
                    await sslStream.AuthenticateAsServerAsync(serverCertificate,
                        _tcpSocketFeederConfiguration.ClientCertificateRequired,
                        _tcpSocketFeederConfiguration.EnabledSslProtocols,
                        _tcpSocketFeederConfiguration.CheckCertificateRevocation);

                    DisplaySecurityLevel(sslStream);
                    DisplaySecurityServices(sslStream);
                    DisplayCertificateInformation(sslStream);
                    DisplayStreamProperties(sslStream);

                    if (_tcpSocketFeederConfiguration.ReadTimeout is not null)
                        sslStream.ReadTimeout = _tcpSocketFeederConfiguration.ReadTimeout.Value;

                    if (_tcpSocketFeederConfiguration.WriteTimeout is not null)
                        sslStream.WriteTimeout = _tcpSocketFeederConfiguration.WriteTimeout.Value;
                }
                else if (stream is NetworkStream networkStream)
                {
                    if (_tcpSocketFeederConfiguration.ReadTimeout is not null)
                        networkStream.ReadTimeout = _tcpSocketFeederConfiguration.ReadTimeout.Value;

                    if (_tcpSocketFeederConfiguration.WriteTimeout is not null)
                        networkStream.WriteTimeout = _tcpSocketFeederConfiguration.WriteTimeout.Value;
                }

                var buffer = new byte[_tcpSocketFeederConfiguration.BufferSize];

                try
                {
                    List<byte> bytes = [];
                    bool finished;
                    do
                    {
                        var bytesRead = await stream.ReadAsync(buffer);
                        finished = bytesRead > 0 && buffer.Length == eom.Length && buffer.SequenceEqual(eom);
                        if (!finished)
                            bytes.AddRange(buffer);
                    } while (!finished);

                    if (checkAuthentication)
                    {
                        if (!Authenticate(bytes))
                        {
                            client.Close();
                            stream.Close();
                            continue;
                        }

                        checkAuthentication = false;
                        continue;
                    }

                    var tcpSocketFeederMessage = Deserialize(bytes.ToArray()) ??
                                                 throw new NullReferenceException("Received message is null. Please ensure that a valid message is provided.");

#if DEBUG
                    var activityContext = tcpSocketFeederMessage[nameof(ActivityContext)] is ActivityContext ac ? ac : default;
                    var baggage = tcpSocketFeederMessage[nameof(Baggage)] is Baggage b ? b : default;
                    await ReceiveAsync(tcpSocketFeederMessage, activityContext, baggage);
#else
                    await ReceiveAsync(tcpSocketFeederMessage);
#endif

                    ReportHealth(HealthStatus.Healthy);
                }
                catch (Exception exception)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Logger.LogError(exception,
                        "error has occured while consuming messages on port {Port}.",
                        string.Join(',', _tcpSocketFeederConfiguration.Port));

                    throw;
                }
            }

            return;

            bool CheckAllowance(EndPoint? endPoint)
                => _tcpSocketFeederConfiguration.AllowedAddresses is null ||
                   _tcpSocketFeederConfiguration.AllowedAddresses.Length == 0 ||
                   endPoint is IPEndPoint ipEndPoint && _tcpSocketFeederConfiguration.AllowedAddresses.Contains(ipEndPoint.Address.ToString());

            void DisplaySecurityLevel(SslStream stream)
            {
                _logger.LogInformation("Cipher: {CipherAlgorithm} strength {CipherStrength}", stream.CipherAlgorithm, stream.CipherStrength);
                _logger.LogInformation("Hash: {HashAlgorithm} strength {HashStrength}", stream.HashAlgorithm, stream.HashStrength);
                _logger.LogInformation("Key exchange: {KeyExchangeAlgorithm} strength {KeyExchangeStrength}", stream.KeyExchangeAlgorithm, stream.KeyExchangeStrength);
                _logger.LogInformation("Protocol: {SslProtocol}", stream.SslProtocol);
            }

            void DisplaySecurityServices(SslStream stream)
            {
                _logger.LogInformation("Is authenticated: {IsAuthenticated} as server? {IsServer}", stream.IsAuthenticated, stream.IsServer);
                _logger.LogInformation("IsSigned: {IsSigned}", stream.IsSigned);
                _logger.LogInformation("Is Encrypted: {IsEncrypted}", stream.IsEncrypted);
                _logger.LogInformation("Is mutually authenticated: {IsMutuallyAuthenticated}", stream.IsMutuallyAuthenticated);
            }

            void DisplayStreamProperties(SslStream stream)
            {
                _logger.LogInformation("Can read: {CanRead}, write {CanWrite}", stream.CanRead, stream.CanWrite);
                _logger.LogInformation("Can timeout: {CanTimeout}", stream.CanTimeout);
            }

            void DisplayCertificateInformation(SslStream stream)
            {
                _logger.LogInformation("Certificate revocation list checked: {CheckCertRevocationStatus}", stream.CheckCertRevocationStatus);

                if (stream.LocalCertificate != null)
                {
                    _logger.LogInformation("Local cert was issued to {Subject} and is valid from {EffectiveDateString} until {ExpirationDateString}.",
                        stream.LocalCertificate.Subject,
                        stream.LocalCertificate.GetEffectiveDateString(),
                        stream.LocalCertificate.GetExpirationDateString());
                }
                else
                {
                    _logger.LogInformation("Local certificate is null.");
                }

                // Display the properties of the client's certificate.
                if (stream.RemoteCertificate != null)
                {
                    _logger.LogInformation("Remote cert was issued to {Subject} and is valid from {EffectiveDateString} until {ExpirationDateString}.",
                        stream.RemoteCertificate.Subject,
                        stream.RemoteCertificate.GetEffectiveDateString(),
                        stream.RemoteCertificate.GetExpirationDateString());
                }
                else
                {
                    _logger.LogInformation("Remote certificate is null.");
                }
            }

            bool Authenticate(List<byte> bytes)
            {
                var authentication = Encoding.UTF8.GetString(bytes.ToArray());
                if (!authentication.StartsWith(Constants.Authentication))
                {
                    Logger.LogError(nameof(InvalidCredentialException));
                    return false;
                }

                authentication = authentication.Replace(Constants.Authentication, string.Empty);
                var authenticationParts = authentication.Split(Constants.Separator, StringSplitOptions.RemoveEmptyEntries);
                if (authenticationParts.Length != 2 || !authenticationParts[0].StartsWith(Constants.Username) || !authenticationParts[1].StartsWith(Constants.Password))
                {
                    Logger.LogError(nameof(InvalidCredentialException));
                    return false;
                }

                var username = authenticationParts[0].Replace(Constants.Username, string.Empty);
                var password = authenticationParts[1].Replace(Constants.Password, string.Empty);
                if (!username.Equals(_tcpSocketFeederConfiguration.Username) && !password.Equals(_tcpSocketFeederConfiguration.Password))
                {
                    Logger.LogError(nameof(InvalidCredentialException));
                    return false;
                }

                return true;
            }
        }

        protected override void DisposeManagedResources()
        {
            _listener.Stop();
            _listener.Dispose();
        }
    }
}