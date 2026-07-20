using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace ThunderPropagator.Feeders.NATS
{
    internal static class NatsJetStreamMessageSettlement
    {
        public static async ValueTask AckOrNakAsync<TMessage>(
            INatsJSMsg<TMessage> message,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            var acknowledged = false;

            try
            {
                await message.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                acknowledged = true;
            }
            finally
            {
                if (!acknowledged)
                {
                    try
                    {
                        await message.NakAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        logger.LogError(exception, "Failed to negatively acknowledge a NATS JetStream message.");
                    }
                }
            }
        }

        public static async IAsyncEnumerable<TReceivedMessage> YieldAndSettleAsync<TMessage, TReceivedMessage>(
            INatsJSMsg<TMessage> message,
            TReceivedMessage receivedMessage,
            ILogger logger,
            [EnumeratorCancellation] CancellationToken cancellationToken,
            Action<bool>? onSettled = null)
        {
            var acknowledged = false;

            try
            {
                yield return receivedMessage;

                await message.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                acknowledged = true;
            }
            finally
            {
                if (!acknowledged)
                {
                    try
                    {
                        await message.NakAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        logger.LogError(exception, "Failed to negatively acknowledge a NATS JetStream message.");
                    }
                }

                onSettled?.Invoke(acknowledged);
            }
        }
    }
}
