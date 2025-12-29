using RabbitMQ.Client;
using ThunderPropagator.BuildingBlocks.Application.Objects;

namespace ThunderPropagator.Feeviders.RabbitMQ.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class RabbitMQFeeviderConnectionFactory : DisposableObject
    {
        public static Task<IConnection> CreateConnectionAsync(RabbitMQFeeviderConfiguration configuration, CancellationToken cancellationToken = default)
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

            if (configuration.EndpointResolverFactory != null)
                factory.EndpointResolverFactory = configuration.EndpointResolverFactory;

            if (configuration.Ssl != null)
                factory.Ssl = configuration.Ssl;

            if (configuration.TopologyRecoveryFilter != null)
                factory.TopologyRecoveryFilter = configuration.TopologyRecoveryFilter;

            if (configuration.TopologyRecoveryExceptionHandler != null)
                factory.TopologyRecoveryExceptionHandler = configuration.TopologyRecoveryExceptionHandler;

            if (configuration.ClientProperties != null)
                factory.ClientProperties = configuration.ClientProperties;

            if (configuration.CredentialsProvider != null)
                factory.CredentialsProvider = configuration.CredentialsProvider;

            if (configuration.MaxInboundMessageBodySize != null)
                factory.MaxInboundMessageBodySize = configuration.MaxInboundMessageBodySize.Value;

            return factory.CreateConnectionAsync(cancellationToken);
        }

        internal static async Task<(IConnection, IChannel)> InitializeChannelAsync(RabbitMQFeeviderConfiguration configuration, CancellationToken cancellationToken = default)
        {
            var connection = await CreateConnectionAsync(configuration, cancellationToken);
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(configuration.Queue,
                configuration.Durable,
                configuration.Exclusive,
                configuration.AutoDelete,
                configuration.Arguments,
                cancellationToken: cancellationToken);

            return (connection, channel);
        }
    }
}