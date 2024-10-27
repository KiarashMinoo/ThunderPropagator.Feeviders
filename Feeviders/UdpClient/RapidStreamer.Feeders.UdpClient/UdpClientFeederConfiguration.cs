using RapidStreamer.Application.Feeders;

namespace RapidStreamer.Feeders.UdpClient
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
            get => Get<int?>() ?? 1024 * 4;
            set => Set(value);
        }

        public string EndMessageCode
        {
            get => Get<string>() ?? "$E";
            set => Set(value);
        }

        public string[]? AllowedAddresses
        {
            get => Get<string[]>();
            set => Set(value);
        }
    }
}