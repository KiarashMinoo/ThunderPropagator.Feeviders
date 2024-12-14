using DotPulsar;
using RapidStreamer.BuildingBlocks.Application;
using RapidStreamer.BuildingBlocks.Application.Certificate;

namespace RapidStreamer.Feeviders.Pulsar.SharedKernel
{
    public abstract class AbstractPulsarFeevidersConfiguration : ServiceConfiguration
    {
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