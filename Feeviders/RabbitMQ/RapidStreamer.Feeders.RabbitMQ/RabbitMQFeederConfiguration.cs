using RabbitMQ.Client;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.RabbitMQ.SharedKernel;
using System.Security.Authentication;

namespace RapidStreamer.Feeders.RabbitMQ
{
    public abstract class RabbitMQFeederConfiguration : AbstractFeederConfiguration,
        IRabbitMQFeeviderConfiguration
    {
        public string HostName
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public int Port
        {
            get => Get(AmqpTcpEndpoint.UseDefaultPort);
            set => Set(value);
        }

        public SslProtocols? AmqpUriSslProtocols
        {
            get => Get<SslProtocols>();
            set => Set(value);
        }

        public bool? AutomaticRecoveryEnabled
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? DispatchConsumersAsync
        {
            get => Get<bool>();
            set => Set(value);
        }

        public int? ConsumerDispatchConcurrency
        {
            get => Get<int>();
            set => Set(value);
        }

        public TimeSpan? NetworkRecoveryInterval
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? HandshakeContinuationTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? ContinuationTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? RequestedConnectionTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? SocketReadTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? SocketWriteTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public bool? TopologyRecoveryEnabled
        {
            get => Get<bool>();
            set => Set(value);
        }

        public string? UserName
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? Password
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public ushort? RequestedChannelMax
        {
            get => Get<ushort>();
            set => Set(value);
        }

        public uint? RequestedFrameMax
        {
            get => Get<uint>();
            set => Set(value);
        }

        public TimeSpan? RequestedHeartbeat
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public string? VirtualHost
        {
            get => Get<string>();
            set => Set(value);
        }

        public Uri? Uri
        {
            get => Get<Uri>();
            set => Set(value);
        }

        public string? ClientProvidedName
        {
            get => Get<string>();
            set => Set(value);
        }

        public uint? MaxMessageSize
        {
            get => Get<uint>();
            set => Set(value);
        }

        public string Queue
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public bool Durable
        {
            get => Get(false);
            set => Set(value);
        }

        public bool Exclusive
        {
            get => Get(false);
            set => Set(value);
        }

        public bool AutoDelete
        {
            get => Get(true);
            set => Set(value);
        }

        public Dictionary<string, object>? Arguments
        {
            get => Get<Dictionary<string, object>>();
            set => Set(value);
        }

        public bool AutoAck
        {
            get => Get(true);
            set => Set(value);
        }
    }
}