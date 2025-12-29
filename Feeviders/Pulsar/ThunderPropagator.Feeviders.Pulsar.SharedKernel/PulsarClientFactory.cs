using DotPulsar;
using DotPulsar.Abstractions;

namespace ThunderPropagator.Feeviders.Pulsar.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class PulsarClientFactory
    {
        public static IPulsarClient CreateClient(AbstractPulsarFeevidersConfiguration configuration)
        {
            var clientBuilder = PulsarClient.Builder()
                .ServiceUrl(configuration.ServiceUrl ?? throw new ArgumentNullException(nameof(configuration.ServiceUrl)));

            if (configuration.EncryptionPolicy != null)
                clientBuilder = clientBuilder.ConnectionSecurity(configuration.EncryptionPolicy.Value);

            if (configuration.KeepAliveInterval != null)
                clientBuilder = clientBuilder.KeepAliveInterval(configuration.KeepAliveInterval.Value);

            if (!string.IsNullOrWhiteSpace(configuration.ListenerName))
                clientBuilder = clientBuilder.ListenerName(configuration.ListenerName);

            if (configuration.RetryInterval != null)
                clientBuilder = clientBuilder.RetryInterval(configuration.RetryInterval.Value);

            if (configuration.VerifyCertificateAuthority != null)
                clientBuilder = clientBuilder.VerifyCertificateAuthority(configuration.VerifyCertificateAuthority.Value);

            if (configuration.VerifyCertificateName != null)
                clientBuilder = clientBuilder.VerifyCertificateName(configuration.VerifyCertificateName.Value);

            if (configuration.CloseInactiveConnectionsInterval != null)
                clientBuilder = clientBuilder.CloseInactiveConnectionsInterval(configuration.CloseInactiveConnectionsInterval.Value);

            var certificate = configuration.AuthenticateUsingClientCertificate?.Certificate;
            if (certificate != null)
                clientBuilder = clientBuilder.AuthenticateUsingClientCertificate(certificate);

            certificate = configuration.TrustedCertificateAuthority?.Certificate;
            if (certificate != null)
                clientBuilder = clientBuilder.TrustedCertificateAuthority(certificate);

            return clientBuilder.Build();
        }
    }
}