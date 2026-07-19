using Google.Cloud.PubSub.V1;
using ThunderPropagator.Feeders.GcpPubSub;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class PubSubMessageSettlementTests
{
    [Fact]
    public async Task YieldAndSettleAsync_ShouldAckAfterProcessingAdvances()
    {
        var completion = new TaskCompletionSource<SubscriberClient.Reply>(TaskCreationOptions.RunContinuationsAsynchronously);
        var context = new PubSubMessageContext(new PubsubMessage { MessageId = "one" }, completion);
        await using var enumerator = PubSubMessageSettlement.YieldAndSettleAsync(context, "payload", CancellationToken.None).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        Assert.False(completion.Task.IsCompleted);
        Assert.False(await enumerator.MoveNextAsync());
        Assert.Equal(SubscriberClient.Reply.Ack, await completion.Task);
    }

    [Fact]
    public async Task YieldAndSettleAsync_ShouldNackWhenProcessingStops()
    {
        var completion = new TaskCompletionSource<SubscriberClient.Reply>(TaskCreationOptions.RunContinuationsAsynchronously);
        var context = new PubSubMessageContext(new PubsubMessage { MessageId = "two" }, completion);
        var enumerator = PubSubMessageSettlement.YieldAndSettleAsync(context, "payload", CancellationToken.None).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        await enumerator.DisposeAsync();

        Assert.Equal(SubscriberClient.Reply.Nack, await completion.Task);
    }
}
