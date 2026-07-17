using System.Runtime.CompilerServices;
using DotPulsar;
using DotPulsar.Abstractions;
using Microsoft.Extensions.Logging;

namespace ThunderPropagator.Feeders.Pulsar
{
    internal static class PulsarMessageSettlement
    {
        public static async IAsyncEnumerable<TReceivedMessage> YieldAndSettleAsync<TMessage, TReceivedMessage>(
            IConsumer<TMessage> consumer,
            IMessage<TMessage> message,
            TReceivedMessage receivedMessage,
            ILogger logger,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var acknowledged = false;

            try
            {
                yield return receivedMessage;

                await consumer.Acknowledge(message.MessageId, cancellationToken).ConfigureAwait(false);
                acknowledged = true;
            }
            finally
            {
                if (!acknowledged)
                {
                    try
                    {
                        await consumer.RedeliverUnacknowledgedMessages(
                            [message.MessageId],
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        logger.LogError(exception, "Failed to redeliver an unacknowledged Pulsar message.");
                    }
                }
            }
        }
    }
}
