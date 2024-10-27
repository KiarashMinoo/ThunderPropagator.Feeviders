using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.TcpSocket.SharedKernel;
using System.Security.Authentication;

namespace RapidStreamer.Feeders.TcpSocket
{
    public abstract class TcpSocketFeederConfiguration : AbstractFeederConfiguration,
        ITcpSocketFeeviderConfiguration
    {
        public bool? Ssl
        {
            get => Get<bool>();
            set => Set(value);
        }

        public string? CertFile
        {
            get => Get<string>();
            set => Set(value);
        }

        public bool ClientCertificateRequired
        {
            get => Get(false);
            set => Set(value);
        }

        public SslProtocols EnabledSslProtocols
        {
            get => Get(SslProtocols.Tls12);
            set => Set(value);
        }

        public bool CheckCertificateRevocation
        {
            get => Get(false);
            set => Set(value);
        }

        public short Port
        {
            get => Get<short>();
            set => Set(value);
        }

        public int BufferSize
        {
            get => Get<int?>() ?? 1024 * 4;
            set => Set(value);
        }

        public string? Username
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? Password
        {
            get => Get<string>();
            set => Set(value);
        }

        public int? ReadTimeout
        {
            get => Get<int>();
            set => Set(value);
        }

        public int? WriteTimeout
        {
            get => Get<int>();
            set => Set(value);
        }

        public string[]? AllowedAddresses
        {
            get => Get<string[]>();
            set => Set(value);
        }
    }
}