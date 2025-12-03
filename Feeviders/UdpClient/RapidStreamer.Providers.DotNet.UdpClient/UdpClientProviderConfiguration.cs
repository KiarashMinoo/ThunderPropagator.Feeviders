using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.UdpClient
{
    public abstract class UdpClientProviderConfiguration : AbstractProviderConfiguration
    {
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
            get => Get<int?>() ?? 65535;
            set => Set(value);
        }

        public string? EncryptionKey
        {
            get => Get<string>();
            set => Set(value);
        }

        public bool EnableEncryption
        {
            get => Get<bool?>() ?? false;
            set => Set(value);
        }
    }
}