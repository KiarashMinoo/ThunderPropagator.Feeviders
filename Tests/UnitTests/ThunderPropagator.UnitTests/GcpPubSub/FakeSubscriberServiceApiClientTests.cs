using Google.Cloud.PubSub.V1;
using NSubstitute;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class FakeSubscriberServiceApiClientTests
{
    [Fact]
    public async Task SubscriberClientImpl_ShouldAcceptFakeServiceApiClient()
    {
        var fakeSubscriberServiceApiClient = Substitute.For<SubscriberServiceApiClient>();
        await using var subscriber = new SubscriberClientImpl(
            SubscriptionName.FromProjectSubscription("test-project", "orders"),
            [fakeSubscriberServiceApiClient],
            new SubscriberClient.Settings(),
            () => Task.CompletedTask);

        Assert.NotNull(subscriber);
    }
}
