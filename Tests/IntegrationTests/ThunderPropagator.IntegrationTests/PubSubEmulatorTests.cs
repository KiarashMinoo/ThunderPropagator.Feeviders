using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging;
using NSubstitute;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.Providers.DotNet.GcpPubSub;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using Xunit;

namespace ThunderPropagator.IntegrationTests.GcpPubSub;

public sealed class PubSubEmulatorTests : IAsyncLifetime
{
    private const int PubSubPort = 8085;
    private readonly IContainer _container = new ContainerBuilder("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators")
        .WithPortBinding(PubSubPort, true)
        .WithCommand("gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085", "--project=test-project")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(PubSubPort))
        .Build();

    public Task InitializeAsync() => _container.StartAsync();

    public async Task DisposeAsync()
    {
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", null);
        await _container.DisposeAsync();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Provider_ShouldPublishToPubSubEmulator()
    {
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", $"{_container.Hostname}:{_container.GetMappedPublicPort(PubSubPort)}");
        var topicName = TopicName.FromProjectTopic("test-project", $"orders-{Guid.NewGuid():N}");
        var subscriptionName = SubscriptionName.FromProjectSubscription("test-project", $"orders-{Guid.NewGuid():N}");
        var publisherApi = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var subscriberApi = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        publisherApi.CreateTopic(new Topic { TopicName = topicName });
        subscriberApi.CreateSubscription(new Subscription { SubscriptionName = subscriptionName, TopicAsTopicName = topicName, AckDeadlineSeconds = 10 });
        var serializer = Substitute.For<IFeederMessageSerializer<TestMessage, TestConfiguration>>();
        serializer.SerializeToBytes(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>()).Returns([1, 2, 3]);
        IServiceProvider services = new TestServiceProvider(Substitute.For<ILoggerFactory>(), serializer);
        await using var provider = new PubSubProvider<TestMessage, TestConfiguration>(new TestConfiguration
        {
            ProjectId = "test-project",
            TopicId = topicName.TopicId
        }, services);

        await provider.ExecuteAsync(new TestMessage(), CancellationToken.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var response = await subscriberApi.PullAsync(new PullRequest { SubscriptionAsSubscriptionName = subscriptionName, MaxMessages = 1 }, timeout.Token);

        var received = Assert.Single(response.ReceivedMessages);
        Assert.Equal(new byte[] { 1, 2, 3 }, received.Message.Data.ToByteArray());
    }

    private sealed class TestServiceProvider(ILoggerFactory loggerFactory, IFeederMessageSerializer<TestMessage, TestConfiguration> serializer) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == typeof(ILoggerFactory) ? loggerFactory :
            serviceType == typeof(IFeederMessageSerializer<TestMessage, TestConfiguration>) ? serializer : null;
    }

    public sealed class TestMessage : PubSubProviderMessage;
    public sealed class TestConfiguration : PubSubProviderConfiguration;
}
