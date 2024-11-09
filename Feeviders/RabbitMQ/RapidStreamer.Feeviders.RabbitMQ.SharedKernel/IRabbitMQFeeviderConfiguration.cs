using System.Security.Authentication;
using RabbitMQ.Client;

namespace RapidStreamer.Feeviders.RabbitMQ.SharedKernel
{
    public interface IRabbitMQFeeviderConfiguration
    {
        string HostName { get; set; }
        int Port { get; set; }

        SslProtocols? AmqpUriSslProtocols { get; set; }
        IEnumerable<IAuthMechanismFactory>? AuthMechanisms { get; set; }
        bool? AutomaticRecoveryEnabled { get; set; }
        ushort? ConsumerDispatchConcurrency { get; set; }
        TimeSpan? NetworkRecoveryInterval { get; set; }
        TimeSpan? HandshakeContinuationTimeout { get; set; }
        TimeSpan? ContinuationTimeout { get; set; }
        Func<IEnumerable<AmqpTcpEndpoint>, IEndpointResolver>? EndpointResolverFactory { get; set; }
        TimeSpan? RequestedConnectionTimeout { get; set; }
        TimeSpan? SocketReadTimeout { get; set; }
        TimeSpan? SocketWriteTimeout { get; set; }
        SslOption? Ssl { get; set; }
        bool? TopologyRecoveryEnabled { get; set; }
        TopologyRecoveryFilter? TopologyRecoveryFilter { get; set; }
        TopologyRecoveryExceptionHandler? TopologyRecoveryExceptionHandler { get; set; }
        IDictionary<string, object?>? ClientProperties { get; set; }
        string? UserName { get; set; }
        string? Password { get; set; }
        ICredentialsProvider? CredentialsProvider { get; set; }
        ushort? RequestedChannelMax { get; set; }
        uint? RequestedFrameMax { get; set; }
        TimeSpan? RequestedHeartbeat { get; set; }
        string? VirtualHost { get; set; }
        uint? MaxInboundMessageBodySize { get; set; }
        Uri? Uri { get; set; }
        string? ClientProvidedName { get; set; }
    }
}