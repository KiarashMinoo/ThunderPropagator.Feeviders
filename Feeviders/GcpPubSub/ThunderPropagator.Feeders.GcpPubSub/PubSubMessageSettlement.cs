using System.Runtime.CompilerServices;
using Google.Cloud.PubSub.V1;

namespace ThunderPropagator.Feeders.GcpPubSub;

internal static class PubSubMessageSettlement
{
    public static async IAsyncEnumerable<TReceivedMessage> YieldAndSettleAsync<TReceivedMessage>(PubSubMessageContext context, TReceivedMessage receivedMessage, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var settled = false;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return receivedMessage;
            context.ProcessingCompleted.TrySetResult(SubscriberClient.Reply.Ack);
            settled = true;
        }
        finally
        {
            if (!settled)
                context.ProcessingCompleted.TrySetResult(SubscriberClient.Reply.Nack);
        }
    }
}
