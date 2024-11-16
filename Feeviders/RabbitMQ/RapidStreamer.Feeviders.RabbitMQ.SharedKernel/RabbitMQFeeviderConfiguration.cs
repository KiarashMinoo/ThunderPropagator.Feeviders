using System.Security.Authentication;
using NJsonSchema.Annotations;
using RabbitMQ.Client;
using RapidStreamer.BuildingBlocks.Application;
using RapidStreamer.BuildingBlocks.Application.Helpers;

namespace RapidStreamer.Feeviders.RabbitMQ.SharedKernel
{
    public abstract class RabbitMQFeeviderConfiguration : ServiceConfiguration, IRabbitMQFeeviderConfiguration
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

        public IEnumerable<IAuthMechanismFactory>? AuthMechanisms
        {
            get => Get<IEnumerable<IAuthMechanismFactory>>();
            set => Set(value);
        }

        public bool? AutomaticRecoveryEnabled
        {
            get => Get<bool>();
            set => Set(value);
        }

        public ushort? ConsumerDispatchConcurrency
        {
            get => Get<ushort>();
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

        [JsonSchemaIgnore]
        public Func<IEnumerable<AmqpTcpEndpoint>, IEndpointResolver>? EndpointResolverFactory
        {
            get => Get<Func<IEnumerable<AmqpTcpEndpoint>, IEndpointResolver>>();
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

        public SslOption? Ssl
        {
            get => Get<SslOption>();
            set => Set(value);
        }

        public bool? TopologyRecoveryEnabled
        {
            get => Get<bool>();
            set => Set(value);
        }

        [JsonSchemaIgnore]
        public TopologyRecoveryFilter? TopologyRecoveryFilter
        {
            get => Get<TopologyRecoveryFilter>();
            set => Set(value);
        }

        [JsonSchemaIgnore]
        public TopologyRecoveryExceptionHandler? TopologyRecoveryExceptionHandler
        {
            get => Get<TopologyRecoveryExceptionHandler>();
            set => Set(value);
        }

        public IDictionary<string, object?>? ClientProperties
        {
            get => Get<IDictionary<string, object?>>();
            set => Set(value);
        }

        public string? UserName
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? Password
        {
            get => Get<string>();
            set => Set(value);
        }

        public ICredentialsProvider? CredentialsProvider
        {
            get => Get<ICredentialsProvider>();
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

        public uint? MaxInboundMessageBodySize
        {
            get => Get<uint>();
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

        //
        public string Queue
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

        public Dictionary<string, object?>? Arguments
        {
            get => Get(nameof(Arguments)).FromNJson<Dictionary<string, object?>?>();
            set => Set(nameof(Arguments), value.ToNJson());
        }

        public bool AutoAck
        {
            get => Get(true);
            set => Set(value);
        }
    }
}