using RabbitMQ.Client;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Feeviders.RabbitMQ.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Security.Authentication;

namespace RapidStreamer.Providers.DotNet.RabbitMQ
{
    public abstract class RabbitMQProviderConfiguration : AbstractProviderConfiguration,
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

        public required string Queue
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public string Exchange
        {
            get => Get(string.Empty);
            set => Set(value);
        }

        public string RoutingKey
        {
            get => Get<string>() ?? Queue;
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
            get => Get(false);
            set => Set(value);
        }

        public Dictionary<string, object>? Arguments
        {
            get => Get(nameof(Arguments)).FromNJson<Dictionary<string, object>?>();
            set => Set(nameof(Arguments), value.ToNJson());
        }

        public bool AutoAck
        {
            get => Get(true);
            set => Set(value);
        }
    }
}