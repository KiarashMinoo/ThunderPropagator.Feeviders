using RabbitMQ.Client;
using RapidStreamer.BuildingBlocks.Application.Objects;

namespace RapidStreamer.Feeviders.RabbitMQ.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class RabbitMQFeederConnectionFactory : DisposableObject
    {
        public static IConnection CreateConnection(IRabbitMQFeeviderConfiguration configuration)
        {
            var factory = new ConnectionFactory
            {
                HostName = configuration.HostName,
                Port = configuration.Port
            };

            if (configuration.AmqpUriSslProtocols != null)
                factory.AmqpUriSslProtocols = configuration.AmqpUriSslProtocols.Value;

            if (configuration.AutomaticRecoveryEnabled != null)
                factory.AutomaticRecoveryEnabled = configuration.AutomaticRecoveryEnabled.Value;

            if (configuration.DispatchConsumersAsync != null)
                factory.DispatchConsumersAsync = configuration.DispatchConsumersAsync.Value;

            if (configuration.ConsumerDispatchConcurrency != null)
                factory.ConsumerDispatchConcurrency = configuration.ConsumerDispatchConcurrency.Value;

            if (configuration.NetworkRecoveryInterval != null)
                factory.NetworkRecoveryInterval = configuration.NetworkRecoveryInterval.Value;

            if (configuration.HandshakeContinuationTimeout != null)
                factory.HandshakeContinuationTimeout = configuration.HandshakeContinuationTimeout.Value;

            if (configuration.ContinuationTimeout != null)
                factory.ContinuationTimeout = configuration.ContinuationTimeout.Value;

            if (configuration.RequestedConnectionTimeout != null)
                factory.RequestedConnectionTimeout = configuration.RequestedConnectionTimeout.Value;

            if (configuration.SocketReadTimeout != null)
                factory.SocketReadTimeout = configuration.SocketReadTimeout.Value;

            if (configuration.SocketWriteTimeout != null)
                factory.SocketWriteTimeout = configuration.SocketWriteTimeout.Value;

            if (configuration.TopologyRecoveryEnabled != null)
                factory.TopologyRecoveryEnabled = configuration.TopologyRecoveryEnabled.Value;

            if (!string.IsNullOrWhiteSpace(configuration.UserName))
                factory.UserName = configuration.UserName;

            if (!string.IsNullOrWhiteSpace(configuration.Password))
                factory.Password = configuration.Password;

            if (configuration.RequestedChannelMax != null)
                factory.RequestedChannelMax = configuration.RequestedChannelMax.Value;

            if (configuration.RequestedFrameMax != null)
                factory.RequestedFrameMax = configuration.RequestedFrameMax.Value;

            if (configuration.RequestedHeartbeat != null)
                factory.RequestedHeartbeat = configuration.RequestedHeartbeat.Value;

            if (!string.IsNullOrWhiteSpace(configuration.VirtualHost))
                factory.VirtualHost = configuration.VirtualHost;

            if (configuration.Uri != null)
                factory.Uri = configuration.Uri;

            if (!string.IsNullOrWhiteSpace(configuration.ClientProvidedName))
                factory.ClientProvidedName = configuration.ClientProvidedName;

            if (configuration.MaxMessageSize != null)
                factory.MaxMessageSize = configuration.MaxMessageSize.Value;

            return factory.CreateConnection();
        }
    }
}