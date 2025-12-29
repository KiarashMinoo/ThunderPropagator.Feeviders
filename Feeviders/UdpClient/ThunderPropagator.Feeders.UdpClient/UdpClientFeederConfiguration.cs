using ThunderPropagator.Application.Feeders;

namespace ThunderPropagator.Feeders.UdpClient
{
    public abstract class UdpClientFeederConfiguration : AbstractFeederConfiguration
    {
        public short Port
        {
            get => Get<short>();
            set => Set(value);
        }

        public int BufferSize
        {
            get => Get<int?>() ?? 65535;
            set => Set(value);
        }

        public string[]? AllowedAddresses
        {
            get => Get<string[]>();
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