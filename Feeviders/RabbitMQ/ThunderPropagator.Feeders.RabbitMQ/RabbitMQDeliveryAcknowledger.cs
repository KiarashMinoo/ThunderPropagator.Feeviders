using RabbitMQ.Client;

namespace ThunderPropagator.Feeders.RabbitMQ
{
    internal static class RabbitMQDeliveryAcknowledger
    {
        public static ValueTask AcknowledgeAsync(
            IChannel channel,
            ulong deliveryTag,
            bool autoAck,
            CancellationToken cancellationToken)
        {
            return autoAck
                ? ValueTask.CompletedTask
                : channel.BasicAckAsync(deliveryTag, false, cancellationToken);
        }

        public static ValueTask NegativeAcknowledgeAsync(
            IChannel channel,
            ulong deliveryTag,
            bool autoAck,
            bool requeue,
            CancellationToken cancellationToken)
        {
            return autoAck
                ? ValueTask.CompletedTask
                : channel.BasicNackAsync(deliveryTag, false, requeue, cancellationToken);
        }
    }
}
