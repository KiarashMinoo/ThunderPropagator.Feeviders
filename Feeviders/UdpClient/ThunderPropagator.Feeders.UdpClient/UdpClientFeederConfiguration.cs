using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.Inbox;

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

        /// <summary>Opt-in transactional Inbox configuration for this Feevider. Disabled by default.</summary>
        public InboxOptions Inbox
        {
            get => Get(new InboxOptions());
            set => Set(value);
        }
    }
}