using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace ThunderPropagator.Feeders.AzureServiceBus;

internal static class ServiceBusMessageSettlement
{
    public static IAsyncEnumerable<TReceivedMessage> YieldAndSettleAsync<TReceivedMessage>(
        ProcessMessageEventArgs eventArgs,
        int maxDeliveryCount,
        TReceivedMessage receivedMessage,
        ILogger logger,
        CancellationToken cancellationToken) =>
        YieldAndSettleAsync(
            eventArgs.Message,
            maxDeliveryCount,
            receivedMessage,
            token => eventArgs.CompleteMessageAsync(eventArgs.Message, token),
            token => eventArgs.AbandonMessageAsync(eventArgs.Message, cancellationToken: token),
            token => eventArgs.DeadLetterMessageAsync(eventArgs.Message, "MaxDeliveryCountExceeded", "Message processing failed at the configured delivery limit.", token),
            logger,
            cancellationToken);

    internal static async IAsyncEnumerable<TReceivedMessage> YieldAndSettleAsync<TReceivedMessage>(
        ServiceBusReceivedMessage message,
        int maxDeliveryCount,
        TReceivedMessage receivedMessage,
        Func<CancellationToken, Task> completeAsync,
        Func<CancellationToken, Task> abandonAsync,
        Func<CancellationToken, Task> deadLetterAsync,
        ILogger logger,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var settled = false;
        try
        {
            yield return receivedMessage;
            await completeAsync(cancellationToken).ConfigureAwait(false);
            settled = true;
        }
        finally
        {
            if (!settled)
            {
                try
                {
                    if (message.DeliveryCount >= maxDeliveryCount)
                        await deadLetterAsync(cancellationToken).ConfigureAwait(false);
                    else
                        await abandonAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "Failed to negatively settle Azure Service Bus message {MessageId}.", message.MessageId);
                }
            }
        }
    }
}
