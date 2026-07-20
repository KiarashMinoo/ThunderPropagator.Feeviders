using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;

namespace ThunderPropagator.Feeders.GcpPubSub;

internal
#if !DEBUG
    sealed
#endif
    partial class PubSubFeeder<TChannel, TMessage, TConfiguration> : IterativeFeeder<TChannel, TMessage, TConfiguration>, IFeature
    where TChannel : class, IChannel
    where TMessage : PubSubFeederMessage
    where TConfiguration : PubSubFeederConfiguration
{
    private static partial class Log
    {
        [LoggerMessage(EventId = 5100, Level = LogLevel.Information, Message = "GCP Pub/Sub exactly-once delivery is expected for subscription {SubscriptionId}; enable it on the subscription.")]
        public static partial void ExactlyOnceDeliveryExpected(ILogger logger, string subscriptionId);

        [LoggerMessage(EventId = 5101, Level = LogLevel.Error, Message = "GCP Pub/Sub subscriber failed for subscription {SubscriptionId}.")]
        public static partial void SubscriberFailed(ILogger logger, Exception exception, string subscriptionId);
    }

    private readonly Channel<PubSubMessageContext> _messages;
    private SubscriberClient? _subscriber;
    private Task? _subscriberTask;

    public PubSubFeeder(TChannel channel, TConfiguration feederConfiguration, IFeederHandler<TChannel, TMessage> feederHandler, IServiceProvider serviceProvider)
        : base(channel, feederConfiguration, feederHandler, serviceProvider)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(feederConfiguration.ProjectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(feederConfiguration.SubscriptionId);
        ArgumentOutOfRangeException.ThrowIfLessThan(feederConfiguration.MaxOutstandingMessages, 1);
        _messages = System.Threading.Channels.Channel.CreateBounded<PubSubMessageContext>(new BoundedChannelOptions(feederConfiguration.MaxOutstandingMessages)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
        HealthName = $"feeder_{nameof(GcpPubSub)}_{feederConfiguration.SubscriptionId}";
        HealthTags = [.. HealthTags, nameof(GcpPubSub), feederConfiguration.SubscriptionId];
    }

    protected override async Task StartingAsync(CancellationToken cancellationToken = default)
    {
        _subscriber = await PubSubClientFactory.CreateSubscriberAsync(FeederConfiguration.ProjectId, FeederConfiguration.SubscriptionId, FeederConfiguration.MaxOutstandingMessages, FeederConfiguration, cancellationToken).ConfigureAwait(false);
        _subscriberTask = _subscriber.StartAsync(HandleMessageAsync);
        _ = ObserveSubscriberAsync(_subscriberTask);
        if (FeederConfiguration.ExactlyOnceDelivery)
            Log.ExactlyOnceDeliveryExpected(Logger, FeederConfiguration.SubscriptionId);
        await base.StartingAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async IAsyncEnumerable<FeederReceivedMessage<TMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var context = await _messages.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        var message = context.Message;
        var propagation = PubSubMessagePropagation.Extract(message.Attributes);
        var receivedMessage = new FeederReceivedMessage<TMessage>(
            Deserialize(message.Data.ToStringUtf8()),
            propagation.ActivityContext,
            propagation.Baggage,
            new Dictionary<string, object?>
            {
                { nameof(message.MessageId), message.MessageId },
                { nameof(message.OrderingKey), message.OrderingKey },
                { nameof(message.PublishTime), message.PublishTime?.ToDateTime() }
            });
        await foreach (var settledMessage in PubSubMessageSettlement.YieldAndSettleAsync(context, receivedMessage, cancellationToken))
            yield return settledMessage;
    }

    private async Task<SubscriberClient.Reply> HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken)
    {
        var context = new PubSubMessageContext(message, new TaskCompletionSource<SubscriberClient.Reply>(TaskCreationOptions.RunContinuationsAsynchronously));
        try
        {
            await _messages.Writer.WriteAsync(context, cancellationToken).ConfigureAwait(false);
            return await context.ProcessingCompleted.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            context.ProcessingCompleted.TrySetResult(SubscriberClient.Reply.Nack);
            return SubscriberClient.Reply.Nack;
        }
        catch (ChannelClosedException)
        {
            context.ProcessingCompleted.TrySetResult(SubscriberClient.Reply.Nack);
            return SubscriberClient.Reply.Nack;
        }
    }

    private async Task ObserveSubscriberAsync(Task subscriberTask)
    {
        try
        {
            await subscriberTask.ConfigureAwait(false);
            _messages.Writer.TryComplete();
        }
        catch (OperationCanceledException)
        {
            _messages.Writer.TryComplete();
        }
        catch (Exception exception)
        {
            Log.SubscriberFailed(Logger, exception, FeederConfiguration.SubscriptionId);
            _messages.Writer.TryComplete(exception);
        }
    }

    protected override async ValueTask DisposeManagedResourcesAsync()
    {
        _messages.Writer.TryComplete();
        while (_messages.Reader.TryRead(out var context))
            context.ProcessingCompleted.TrySetResult(SubscriberClient.Reply.Nack);
        if (_subscriber is not null)
        {
            await _subscriber.StopAsync(new SubscriberClient.ShutdownOptions { Mode = SubscriberClient.ShutdownMode.WaitForProcessing, Timeout = TimeSpan.FromSeconds(30) }, CancellationToken.None).ConfigureAwait(false);
            await _subscriber.DisposeAsync().ConfigureAwait(false);
        }
        if (_subscriberTask is not null)
        {
            try { await _subscriberTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
    }
}
