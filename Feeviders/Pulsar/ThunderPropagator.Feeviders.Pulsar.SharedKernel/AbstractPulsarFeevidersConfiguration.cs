using DotPulsar;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Certificate;

namespace ThunderPropagator.Feeviders.Pulsar.SharedKernel
{
    public abstract class AbstractPulsarFeevidersConfiguration : ServiceConfiguration
    {
        public bool IsEnabled
        {
            get => Get<bool>();
            set => Set(value);
        }
        
        //Client
        public Uri ServiceUrl
        {
            get => Get<Uri>()!;
            set => Set(value);
        }

        public EncryptionPolicy? EncryptionPolicy
        {
            get => Get<EncryptionPolicy>();
            set => Set(value);
        }

        public TimeSpan? KeepAliveInterval
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public string? ListenerName
        {
            get => Get<string>();
            set => Set(value);
        }

        public TimeSpan? RetryInterval
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public bool? VerifyCertificateAuthority
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? VerifyCertificateName
        {
            get => Get<bool>();
            set => Set(value);
        }

        public TimeSpan? CloseInactiveConnectionsInterval
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public CertificateModel? AuthenticateUsingClientCertificate
        {
            get => Get<CertificateModel>();
            set => Set(value);
        }

        public CertificateModel? TrustedCertificateAuthority
        {
            get => Get<CertificateModel>();
            set => Set(value);
        }
    }
}