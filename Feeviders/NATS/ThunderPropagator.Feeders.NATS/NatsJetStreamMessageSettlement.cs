using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace ThunderPropagator.Feeders.NATS
{
    internal static class NatsJetStreamMessageSettlement
    {
        public static async IAsyncEnumerable<TReceivedMessage> YieldAndSettleAsync<TMessage, TReceivedMessage>(
            INatsJSMsg<TMessage> message,
            TReceivedMessage receivedMessage,
            ILogger logger,
            [EnumeratorCancellation] CancellationToken cancellationToken)
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
                        await message.NakAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        logger.LogError(exception, "Failed to negatively acknowledge a NATS JetStream message.");
                    }
                }
            }
        }
    }
}
