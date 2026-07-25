using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Features;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

namespace ThunderPropagator.Feeders.AzureServiceBus;

internal
#if !DEBUG
    sealed
#endif
    partial class ServiceBusFeeder<TChannel, TMessage, TConfiguration> : IterativeFeeder<TChannel, TMessage, TConfiguration>, IFeature
    where TChannel : class, IChannel
    where TMessage : ServiceBusFeederMessage
    where TConfiguration : ServiceBusFeederConfiguration
{
    private static partial class Log
    {
        [LoggerMessage(EventId = 5200, Level = LogLevel.Error, Message = "Azure Service Bus processor failed for {EntityPath} at {ErrorSource}.")]
        public static partial void ProcessorError(ILogger logger, Exception exception, string entityPath, ServiceBusErrorSource errorSource);
    }

    private readonly ServiceBusClient _client;
    private readonly ServiceBusProcessor _processor;
    private readonly Channel<ServiceBusMessageContext> _messages = System.Threading.Channels.Channel.CreateUnbounded<ServiceBusMessageContext>(
        new UnboundedChannelOptions { SingleReader = true });

    public ServiceBusFeeder(
        TChannel channel,
        TConfiguration feederConfiguration,
        IFeederHandler<TChannel, TMessage> feederHandler,
        IServiceProvider serviceProvider)
        : base(channel, feederConfiguration, feederHandler, serviceProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(feederConfiguration.MaxConcurrentCalls, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(feederConfiguration.MaxDeliveryCount, 1);

        _client = ServiceBusClientFactory.Create(feederConfiguration);
        var entityPath = ServiceBusEntityPath.Parse(feederConfiguration.EntityPath);
        var options = new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = feederConfiguration.MaxConcurrentCalls,
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        };
        _processor = entityPath.SubscriptionName is null
            ? _client.CreateProcessor(entityPath.EntityName, options)
            : _client.CreateProcessor(entityPath.EntityName, entityPath.SubscriptionName, options);
        _processor.ProcessMessageAsync += HandleMessageAsync;
        _processor.ProcessErrorAsync += HandleErrorAsync;

        HealthName = $"feeder_{nameof(AzureServiceBus)}_{feederConfiguration.EntityPath}";
        HealthTags = [.. HealthTags, nameof(AzureServiceBus), feederConfiguration.EntityPath];
    }

    protected override async Task StartingAsync(CancellationToken cancellationToken = default)
    {
        await _processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
        await base.StartingAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async IAsyncEnumerable<FeederReceivedMessage<TMessage>> ReceiveAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var context = await _messages.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var message = context.EventArgs.Message;
            var propagation = ServiceBusMessagePropagation.Extract(message.ApplicationProperties);
            var feederMessage = Deserialize(message.Body.ToString());
            var receivedMessage = new FeederReceivedMessage<TMessage>(
                feederMessage,
                propagation.ActivityContext,
                propagation.Baggage,
                new Dictionary<string, object?>
                {
                    { nameof(message.MessageId), message.MessageId },
                    { nameof(message.CorrelationId), message.CorrelationId },
                    { nameof(message.DeliveryCount), message.DeliveryCount }
                });

            await foreach (var settledMessage in ServiceBusMessageSettlement.YieldAndSettleAsync(
                               context.EventArgs,
                               FeederConfiguration.MaxDeliveryCount,
                               receivedMessage,
                               Logger,
                               cancellationToken))
            {
                yield return settledMessage;
            }
        }
        finally
        {
            context.ProcessingCompleted.TrySetResult();
        }
    }

    private async Task HandleMessageAsync(ProcessMessageEventArgs eventArgs)
    {
        var context = new ServiceBusMessageContext(
            eventArgs,
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        await _messages.Writer.WriteAsync(context, eventArgs.CancellationToken).ConfigureAwait(false);
        await context.ProcessingCompleted.Task.WaitAsync(eventArgs.CancellationToken).ConfigureAwait(false);
    }

    private Task HandleErrorAsync(ProcessErrorEventArgs eventArgs)
    {
        Log.ProcessorError(Logger, eventArgs.Exception, eventArgs.EntityPath, eventArgs.ErrorSource);
        return Task.CompletedTask;
    }

    protected override async ValueTask DisposeManagedResourcesAsync()
    {
        _messages.Writer.TryComplete();
        while (_messages.Reader.TryRead(out var context))
            context.ProcessingCompleted.TrySetCanceled();

        if (_processor.IsProcessing)
            await _processor.StopProcessingAsync().ConfigureAwait(false);
        _processor.ProcessMessageAsync -= HandleMessageAsync;
        _processor.ProcessErrorAsync -= HandleErrorAsync;
        await _processor.DisposeAsync().ConfigureAwait(false);
        await _client.DisposeAsync().ConfigureAwait(false);
    }
}
