using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.Providers.DotNet.AzureServiceBus;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class ServiceBusProviderTests
{
    [Fact]
    public async Task ExecuteAsync_ShouldSendSerializedMessageThroughMockedClient()
    {
        var serializer = Substitute.For<IFeederMessageSerializer<TestMessage, TestConfiguration>>();
        serializer.SerializeToBytes(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>()).Returns([1, 2, 3]);
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(serializer)
            .BuildServiceProvider();
        var sender = Substitute.For<ServiceBusSender>();
        var client = Substitute.For<ServiceBusClient>();
        client.CreateSender("orders").Returns(sender);
        var provider = new ServiceBusProvider<TestMessage, TestConfiguration>(
            new TestConfiguration { EntityPath = "orders" },
            services,
            client);

        await provider.ExecuteAsync(new TestMessage(), CancellationToken.None);

        await sender.Received(1).SendMessageAsync(
            Arg.Is<ServiceBusMessage>(message => message != null && message.Body.ToArray().SequenceEqual(new byte[] { 1, 2, 3 })),
            CancellationToken.None);
        provider.Dispose();
    }

    public sealed class TestMessage : ServiceBusProviderMessage;

    public sealed class TestConfiguration : ServiceBusProviderConfiguration;
}
