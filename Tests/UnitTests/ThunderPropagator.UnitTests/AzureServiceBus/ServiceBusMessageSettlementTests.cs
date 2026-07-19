using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using NSubstitute;
using ThunderPropagator.Feeders.AzureServiceBus;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class ServiceBusMessageSettlementTests
{
    [Fact]
    public async Task YieldAndSettleAsync_ShouldCompleteAfterProcessingAdvances()
    {
        var calls = new List<string>();
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(messageId: "one", deliveryCount: 1);
        await using var enumerator = CreateEnumerable(message, 3, calls).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Empty(calls);
        Assert.False(await enumerator.MoveNextAsync());
        Assert.Equal(["complete"], calls);
    }

    [Fact]
    public async Task YieldAndSettleAsync_ShouldAbandonWhenProcessingStopsBeforeLimit()
    {
        var calls = new List<string>();
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(messageId: "two", deliveryCount: 2);
        var enumerator = CreateEnumerable(message, 3, calls).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        await enumerator.DisposeAsync();

        Assert.Equal(["abandon"], calls);
    }

    [Fact]
    public async Task YieldAndSettleAsync_ShouldDeadLetterAtDeliveryLimit()
    {
        var calls = new List<string>();
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(messageId: "three", deliveryCount: 3);
        var enumerator = CreateEnumerable(message, 3, calls).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        await enumerator.DisposeAsync();

        Assert.Equal(["dead-letter"], calls);
    }

    private static IAsyncEnumerable<string> CreateEnumerable(ServiceBusReceivedMessage message, int maxDeliveryCount, List<string> calls) =>
        ServiceBusMessageSettlement.YieldAndSettleAsync(
            message,
            maxDeliveryCount,
            "payload",
            _ => Record(calls, "complete"),
            _ => Record(calls, "abandon"),
            _ => Record(calls, "dead-letter"),
            Substitute.For<ILogger>(),
            CancellationToken.None);

    private static Task Record(List<string> calls, string value)
    {
        calls.Add(value);
        return Task.CompletedTask;
    }
}
