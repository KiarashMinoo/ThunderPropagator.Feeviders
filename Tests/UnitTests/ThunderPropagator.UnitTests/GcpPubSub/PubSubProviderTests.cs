using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.Providers.DotNet.GcpPubSub;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class PubSubProviderTests
{
    [Fact]
    public async Task ExecuteAsync_ShouldPublishSerializedMessageWithOrderingKey()
    {
        var serializer = Substitute.For<IFeederMessageSerializer<TestMessage, TestConfiguration>>();
        serializer.SerializeToBytes(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>()).Returns([1, 2, 3]);
        var services = new ServiceCollection().AddLogging().AddSingleton(serializer).BuildServiceProvider();
        var publisher = Substitute.For<PublisherClient>();
        publisher.PublishAsync(Arg.Any<PubsubMessage>()).Returns(Task.FromResult("message-id"));
        var provider = new PubSubProvider<TestMessage, TestConfiguration>(new TestConfiguration
        {
            ProjectId = "test-project",
            TopicId = "orders",
            OrderingKey = "customer-42"
        }, services, publisher);

        await provider.ExecuteAsync(new TestMessage(), CancellationToken.None);

        await publisher.Received(1).PublishAsync(Arg.Is<PubsubMessage>(message =>
            message != null && message.Data.ToByteArray().SequenceEqual(new byte[] { 1, 2, 3 }) && message.OrderingKey == "customer-42"));
        provider.Dispose();
    }

    public sealed class TestMessage : PubSubProviderMessage;
    public sealed class TestConfiguration : PubSubProviderConfiguration;
}
