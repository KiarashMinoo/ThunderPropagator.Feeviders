using Apache.NMS;
using Apache.NMS.ActiveMQ;

namespace RapidStreamer.Feeviders.ActiveMQ.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class ActiveMQConnectionFactory
    {
        public static IConnection CreateConnection(IActiveMQFeeviderConfiguration configuration)
        {
            var connectionFactory = new ConnectionFactory(configuration.BrokerUri ?? throw new ArgumentNullException(nameof(configuration.BrokerUri)));

            if (!string.IsNullOrWhiteSpace(configuration.ClientId))
                connectionFactory.ClientId = configuration.ClientId;

            if (!string.IsNullOrWhiteSpace(configuration.ClientIdPrefix))
                connectionFactory.ClientIdPrefix = configuration.ClientIdPrefix;

            if (!string.IsNullOrWhiteSpace(configuration.UserName))
                connectionFactory.UserName = configuration.UserName;

            if (!string.IsNullOrWhiteSpace(configuration.Password))
                connectionFactory.Password = configuration.Password;

            if (configuration.UseCompression != null)
                connectionFactory.UseCompression = configuration.UseCompression.Value;

            if (configuration.CopyMessageOnSend != null)
                connectionFactory.CopyMessageOnSend = configuration.CopyMessageOnSend.Value;

            if (configuration.AlwaysSyncSend != null)
                connectionFactory.AlwaysSyncSend = configuration.AlwaysSyncSend.Value;

            if (configuration.AsyncClose != null)
                connectionFactory.AsyncClose = configuration.AsyncClose.Value;

            if (configuration.SendAcksAsync != null)
                connectionFactory.SendAcksAsync = configuration.SendAcksAsync.Value;

            if (configuration.AsyncSend != null)
                connectionFactory.AsyncSend = configuration.AsyncSend.Value;

            if (configuration.DispatchAsync != null)
                connectionFactory.DispatchAsync = configuration.DispatchAsync.Value;

            if (configuration.WatchTopicAdvisories != null)
                connectionFactory.WatchTopicAdvisories = configuration.WatchTopicAdvisories.Value;

            if (configuration.MessagePrioritySupported != null)
                connectionFactory.MessagePrioritySupported = configuration.MessagePrioritySupported.Value;

            if (configuration.RequestTimeout != null)
                connectionFactory.RequestTimeout = configuration.RequestTimeout.Value;

            if (configuration.AcknowledgementMode != null)
                connectionFactory.AcknowledgementMode = configuration.AcknowledgementMode.Value;

            if (configuration.ProducerWindowSize != null)
                connectionFactory.ProducerWindowSize = configuration.ProducerWindowSize.Value;

            if (configuration.OptimizeAcknowledge != null)
                connectionFactory.OptimizeAcknowledge = configuration.OptimizeAcknowledge.Value;

            if (configuration.OptimizeAcknowledgeTimeOut != null)
                connectionFactory.OptimizeAcknowledgeTimeOut = configuration.OptimizeAcknowledgeTimeOut.Value;

            if (configuration.OptimizedAckScheduledAckInterval != null)
                connectionFactory.OptimizedAckScheduledAckInterval = configuration.OptimizedAckScheduledAckInterval.Value;

            if (configuration.UseRetroactiveConsumer != null)
                connectionFactory.UseRetroactiveConsumer = configuration.UseRetroactiveConsumer.Value;

            if (configuration.ExclusiveConsumer != null)
                connectionFactory.ExclusiveConsumer = configuration.ExclusiveConsumer.Value;

            if (configuration.ConsumerFailoverRedeliveryWaitPeriod != null)
                connectionFactory.ConsumerFailoverRedeliveryWaitPeriod = configuration.ConsumerFailoverRedeliveryWaitPeriod.Value;

            if (configuration.CheckForDuplicates != null)
                connectionFactory.CheckForDuplicates = configuration.CheckForDuplicates.Value;

            if (configuration.TransactedIndividualAck != null)
                connectionFactory.TransactedIndividualAck = configuration.TransactedIndividualAck.Value;

            if (configuration.NonBlockingRedelivery != null)
                connectionFactory.NonBlockingRedelivery = configuration.NonBlockingRedelivery.Value;

            if (configuration.AuditDepth != null)
                connectionFactory.AuditDepth = configuration.AuditDepth.Value;

            if (configuration.AuditMaximumProducerNumber != null)
                connectionFactory.AuditMaximumProducerNumber = configuration.AuditMaximumProducerNumber.Value;

            return connectionFactory.CreateConnection();
        }
    }
}