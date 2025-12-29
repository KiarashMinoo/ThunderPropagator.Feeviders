using ThunderPropagator.Feeviders.TcpSocket.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.TcpSocket
{
    public abstract class TcpSocketProviderConfiguration : AbstractProviderConfiguration,
        ITcpSocketFeeviderConfiguration
    {
        public bool? Ssl
        {
            get => Get<bool>();
            set => Set(value);
        }

        public required string Endpoint
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public required short Port
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
    }
}