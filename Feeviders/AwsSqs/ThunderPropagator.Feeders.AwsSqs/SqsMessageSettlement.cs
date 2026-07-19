using System.Runtime.CompilerServices;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace ThunderPropagator.Feeders.AwsSqs
{
    internal static class SqsMessageSettlement
    {
        public static async IAsyncEnumerable<TReceivedMessage> YieldAndSettleAsync<TReceivedMessage>(
            IAmazonSQS client,
            string queueUrl,
            Message message,
            TReceivedMessage receivedMessage,
            ILogger logger,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var acknowledged = false;

            try
            {
                yield return receivedMessage;

                await client.DeleteMessageAsync(queueUrl, message.ReceiptHandle, cancellationToken).ConfigureAwait(false);
                acknowledged = true;
            }
            finally
            {
                if (!acknowledged)
                {
                    try
                    {
                        await client.ChangeMessageVisibilityAsync(queueUrl, message.ReceiptHandle, 0, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        logger.LogError(exception, "Failed to reset the visibility timeout for an unacknowledged SQS message.");
                    }
                }
            }
        }
    }
}
