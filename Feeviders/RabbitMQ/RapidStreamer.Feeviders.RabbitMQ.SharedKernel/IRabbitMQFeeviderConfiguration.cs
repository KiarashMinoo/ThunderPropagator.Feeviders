using System.Security.Authentication;

namespace RapidStreamer.Feeviders.RabbitMQ.SharedKernel
{
    public interface IRabbitMQFeeviderConfiguration
    {
        string HostName { get; set; }
        int Port { get; set; }
        SslProtocols? AmqpUriSslProtocols { get; set; }
        bool? AutomaticRecoveryEnabled { get; set; }
        bool? DispatchConsumersAsync { get; set; }
        int? ConsumerDispatchConcurrency { get; set; }
        TimeSpan? NetworkRecoveryInterval { get; set; }
        TimeSpan? HandshakeContinuationTimeout { get; set; }
        TimeSpan? ContinuationTimeout { get; set; }
        TimeSpan? RequestedConnectionTimeout { get; set; }
        TimeSpan? SocketReadTimeout { get; set; }
        TimeSpan? SocketWriteTimeout { get; set; }
        bool? TopologyRecoveryEnabled { get; set; }
        string? UserName { get; set; }
        string? Password { get; set; }
        ushort? RequestedChannelMax { get; set; }
        uint? RequestedFrameMax { get; set; }
        TimeSpan? RequestedHeartbeat { get; set; }
        string? VirtualHost { get; set; }
        Uri? Uri { get; set; }
        string? ClientProvidedName { get; set; }
        uint? MaxMessageSize { get; set; }
    }
}