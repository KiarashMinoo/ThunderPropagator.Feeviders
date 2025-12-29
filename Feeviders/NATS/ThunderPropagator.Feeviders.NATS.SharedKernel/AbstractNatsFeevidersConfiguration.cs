using System.Text;
using System.Threading.Channels;
using NATS.Client.Core;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Feeviders.NATS.SharedKernel
{
    public abstract class AbstractNatsFeevidersConfiguration : ServiceConfiguration
    {
        public bool IsEnabled
        {
            get => Get<bool>();
            set => Set(value);
        }

        public string Url
        {
            get => Get("nats://localhost:4222");
            set => Set(value);
        }

        public string Name
        {
            get => Get("NATS .NET Client");
            set => Set(value);
        }

        public bool Echo
        {
            get => Get(true);
            set => Set(value);
        }

        public bool Verbose
        {
            get => Get(false);
            set => Set(value);
        }

        public bool Headers
        {
            get => Get(true);
            set => Set(value);
        }

        public NatsAuthOpts AuthOpts
        {
            get => Get(NatsAuthOpts.Default);
            set => Set(value);
        }

        public NatsTlsOpts TlsOpts
        {
            get => Get(NatsTlsOpts.Default);
            set => Set(value);
        }

        public NatsWebSocketOpts WebSocketOpts
        {
            get => Get(NatsWebSocketOpts.Default);
            set => Set(value);
        }

        public int WriterBufferSize
        {
            get => Get(65536);
            set => Set(value);
        }

        public int ReaderBufferSize
        {
            get => Get(65536);
            set => Set(value);
        }

        public bool UseThreadPoolCallback
        {
            get => Get(false);
            set => Set(value);
        }

        public string InboxPrefix
        {
            get => Get("_INBOX");
            set => Set(value);
        }

        public bool NoRandomize
        {
            get => Get(false);
            set => Set(value);
        }

        public TimeSpan PingInterval
        {
            get => Get(TimeSpan.FromMinutes(2.0));
            set => Set(value);
        }

        public int MaxPingOut
        {
            get => Get(2);
            set => Set(value);
        }

        public TimeSpan ReconnectWaitMin
        {
            get => Get(TimeSpan.FromSeconds(2.0));
            set => Set(value);
        }

        public TimeSpan ReconnectJitter
        {
            get => Get(TimeSpan.FromMilliseconds(100.0));
            set => Set(value);
        }

        public TimeSpan ConnectTimeout
        {
            get => Get(TimeSpan.FromSeconds(2.0));
            set => Set(value);
        }

        public int ObjectPoolSize
        {
            get => Get(256);
            set => Set(value);
        }

        public TimeSpan RequestTimeout
        {
            get => Get(TimeSpan.FromSeconds(5.0));
            set => Set(value);
        }

        public TimeSpan CommandTimeout
        {
            get => Get(TimeSpan.FromSeconds(5.0));
            set => Set(value);
        }

        public TimeSpan SubscriptionCleanUpInterval
        {
            get => Get(TimeSpan.FromSeconds(5.0));
            set => Set(value);
        }

        public string HeaderEncoding
        {
            get => Get(nameof(Encoding.ASCII));
            set => Set(value);
        }

        public string SubjectEncoding
        {
            get => Get(nameof(Encoding.ASCII));
            set => Set(value);
        }

        public bool WaitUntilSent
        {
            get => Get(false);
            set => Set(value);
        }

        public int MaxReconnectRetry
        {
            get => Get(-1);
            set => Set(value);
        }

        public TimeSpan ReconnectWaitMax
        {
            get => Get(TimeSpan.FromSeconds(5.0));
            set => Set(value);
        }

        public bool IgnoreAuthErrorAbort
        {
            get => Get(false);
            set => Set(value);
        }

        public int SubPendingChannelCapacity
        {
            get => Get(1024);
            set => Set(value);
        }

        public BoundedChannelFullMode SubPendingChannelFullMode
        {
            get => Get(BoundedChannelFullMode.DropNewest);
            set => Set(value);
        }

        public BoundedChannelFullMode BoundedChannelFullMode
        {
            get => Get(BoundedChannelFullMode.Wait);
            set => Set(value);
        }

        public SerializerType SerializerType
        {
            get => Get(SerializerType.Json);
            set => Set(value);
        }

        public MessagingType MessagingType
        {
            get => Get(MessagingType.Basic);
            set => Set(value);
        }
    }
}