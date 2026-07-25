using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Grpc.Net.Client;

namespace ThunderPropagator.Feeviders.Grpc.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class GrpcChannelFactory
    {
        public static GrpcChannel CreateChannel(AbstractGrpcFeevidersConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration.Endpoint);

            if (!configuration.UseTls)
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            var httpHandler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingDelay = configuration.KeepAliveInterval,
                KeepAlivePingTimeout = configuration.KeepAliveTimeout,
                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests,
                EnableMultipleHttp2Connections = true
            };

            var certificate = configuration.ClientCertificate?.Certificate;
            if (certificate != null)
            {
                httpHandler.SslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = new X509CertificateCollection { certificate }
                };
            }

            return GrpcChannel.ForAddress(configuration.Endpoint, new GrpcChannelOptions
            {
                HttpHandler = httpHandler
            });
        }
    }
}
