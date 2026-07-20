using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.GcpPubSub;

internal
#if !DEBUG
    sealed
#endif
    partial class PubSubProvider<TMessage, TConfiguration> : AbstractProvider<TMessage, TConfiguration>
    where TMessage : PubSubProviderMessage
    where TConfiguration : PubSubProviderConfiguration
{
    private static partial class Log
    {
        [LoggerMessage(EventId = 5102, Level = LogLevel.Error, Message = "Error occurred while publishing a GCP Pub/Sub message to {TopicId}.")]
        public static partial void PublishError(ILogger logger, Exception exception, string topicId);
    }

    private readonly TConfiguration _configuration;
    private readonly PublisherClient _publisher;

    public PubSubProvider(TConfiguration configuration, IServiceProvider serviceProvider)
        : this(configuration, serviceProvider, PubSubClientFactory.CreatePublisher(configuration.ProjectId, configuration.TopicId, !string.IsNullOrWhiteSpace(configuration.OrderingKey), configuration))
    {
    }

    internal PubSubProvider(TConfiguration configuration, IServiceProvider serviceProvider, PublisherClient publisher)
        : base(serviceProvider)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configuration.ProjectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(configuration.TopicId);
        _configuration = configuration;
        _publisher = publisher;
    }

    protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
    {
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFrom(bytes),
            OrderingKey = _configuration.OrderingKey ?? string.Empty
        };
        PubSubMessagePropagation.Inject(message, Activity.Current?.Context, Baggage.Current);
        try
        {
            await _publisher.PublishAsync(message).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Log.PublishError(Logger, exception, _configuration.TopicId);
            throw;
        }
    }

    protected override ValueTask DisposeManagedResourcesAsync() => _publisher.DisposeAsync();
}
