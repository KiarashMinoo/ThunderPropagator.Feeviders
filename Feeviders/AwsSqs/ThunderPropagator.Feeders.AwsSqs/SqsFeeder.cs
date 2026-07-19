using System.Diagnostics;
using System.Runtime.CompilerServices;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Feeviders.AwsSqs.SharedKernel;

namespace ThunderPropagator.Feeders.AwsSqs
{
    internal
#if !DEBUG
        sealed
#endif
        class SqsFeeder<TChannel, TSqsFeederMessage, TSqsFeederConfiguration> : IterativeFeeder<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TSqsFeederMessage : SqsFeederMessage
        where TSqsFeederConfiguration : SqsFeederConfiguration
    {
        private readonly IAmazonSQS _client;

        public SqsFeeder(TChannel channel,
            TSqsFeederConfiguration feederConfiguration,
            IFeederHandler<TChannel, TSqsFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, feederConfiguration, feederHandler, serviceProvider)
        {
            _client = AwsSqsFeeviderConnectionFactory.CreateSqsClient(feederConfiguration);

            HealthName = $"feeder_{nameof(AwsSqs)}_{feederConfiguration.QueueUrl}";
            HealthTags = [.. HealthTags, nameof(AwsSqs), feederConfiguration.QueueUrl];

            Logger.LogInformation(
                "{FeederName}/{ChannelName} on Queue {QueueUrl} has configured.",
                GetType().Name,
                channel.Metadata.ChannelName,
                feederConfiguration.QueueUrl);
        }

        protected override async IAsyncEnumerable<FeederReceivedMessage<TSqsFeederMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var response = await _client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = FeederConfiguration.QueueUrl,
                MaxNumberOfMessages = FeederConfiguration.MaxNumberOfMessages,
                WaitTimeSeconds = FeederConfiguration.WaitTimeSeconds,
                VisibilityTimeout = FeederConfiguration.VisibilityTimeout,
                MessageAttributeNames = ["All"]
            }, cancellationToken).ConfigureAwait(false);

            if (response.Messages.Count == 0)
            {
                await Task.Yield();
                yield break;
            }

            foreach (var message in response.Messages)
            {
                ActivityContext? activityContext = null;
                if (message.MessageAttributes.TryGetValue(nameof(ActivityContext), out var activityContextAttribute))
                    activityContext = activityContextAttribute.StringValue.FromNJsonBase64<ActivityContext>();

                Baggage? baggage = null;
                if (message.MessageAttributes.TryGetValue(nameof(Baggage), out var baggageAttribute))
                    baggage = baggageAttribute.StringValue.FromNJsonBase64<Baggage>();

                var feederMessage = Deserialize(message.Body);

                await foreach (var receivedMessage in SqsMessageSettlement.YieldAndSettleAsync(
                                   _client,
                                   FeederConfiguration.QueueUrl,
                                   message,
                                   new FeederReceivedMessage<TSqsFeederMessage>(feederMessage, activityContext, baggage,
                                       new Dictionary<string, object?>
                                       {
                                           { nameof(message.MessageId), message.MessageId },
                                       }),
                                   Logger,
                                   cancellationToken))
                {
                    yield return receivedMessage;
                }
            }
        }

        protected override ValueTask DisposeManagedResourcesAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
