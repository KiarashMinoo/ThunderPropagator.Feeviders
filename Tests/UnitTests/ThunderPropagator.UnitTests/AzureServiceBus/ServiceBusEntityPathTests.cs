using ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class ServiceBusEntityPathTests
{
    [Fact]
    public void Parse_ShouldSupportQueuePath()
    {
        var result = ServiceBusEntityPath.Parse("orders");

        Assert.Equal("orders", result.EntityName);
        Assert.Null(result.SubscriptionName);
    }

    [Fact]
    public void Parse_ShouldSupportTopicSubscriptionPath()
    {
        var result = ServiceBusEntityPath.Parse("events/accounting");

        Assert.Equal("events", result.EntityName);
        Assert.Equal("accounting", result.SubscriptionName);
    }

    [Theory]
    [InlineData("")]
    [InlineData("topic/subscription/extra")]
    public void Parse_ShouldRejectInvalidPath(string entityPath)
    {
        Assert.ThrowsAny<ArgumentException>(() => ServiceBusEntityPath.Parse(entityPath));
    }
}
