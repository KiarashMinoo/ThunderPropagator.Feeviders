using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;

namespace ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;

internal static class PubSubClientFactory
{
    public static async Task<SubscriberClient> CreateSubscriberAsync(
        string projectId,
        string subscriptionId,
        int maxOutstandingMessages,
        IGcpPubSubFeeviderConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var builder = new SubscriberClientBuilder
        {
            SubscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId),
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction,
            Settings = new SubscriberClient.Settings
            {
                FlowControlSettings = new FlowControlSettings(maxOutstandingMessages, null)
            }
        };
        ApplyCredentials(builder, configuration);
        return await builder.BuildAsync(cancellationToken).ConfigureAwait(false);
    }

    public static PublisherClient CreatePublisher(
        string projectId,
        string topicId,
        bool enableMessageOrdering,
        IGcpPubSubFeeviderConfiguration configuration)
    {
        var builder = new PublisherClientBuilder
        {
            TopicName = TopicName.FromProjectTopic(projectId, topicId),
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction,
            Settings = new PublisherClient.Settings
            {
                EnableMessageOrdering = enableMessageOrdering
            }
        };
        ApplyCredentials(builder, configuration);
        return builder.Build();
    }

    private static void ApplyCredentials(Google.Api.Gax.Grpc.ClientBuilderBase<SubscriberClient> builder, IGcpPubSubFeeviderConfiguration configuration)
    {
        if (!string.IsNullOrWhiteSpace(configuration.ServiceAccountKeyPath))
            builder.GoogleCredential = CredentialFactory.FromFile<ServiceAccountCredential>(configuration.ServiceAccountKeyPath).ToGoogleCredential();
    }

    private static void ApplyCredentials(Google.Api.Gax.Grpc.ClientBuilderBase<PublisherClient> builder, IGcpPubSubFeeviderConfiguration configuration)
    {
        if (!string.IsNullOrWhiteSpace(configuration.ServiceAccountKeyPath))
            builder.GoogleCredential = CredentialFactory.FromFile<ServiceAccountCredential>(configuration.ServiceAccountKeyPath).ToGoogleCredential();
    }
}
