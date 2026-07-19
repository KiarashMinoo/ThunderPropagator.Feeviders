using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using OpenTelemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.AzureServiceBus;

internal
#if !DEBUG
    sealed
#endif
    class ServiceBusProvider<TMessage, TConfiguration> : AbstractProvider<TMessage, TConfiguration>, IServiceBusBatchProvider<TMessage>
    where TMessage : ServiceBusProviderMessage
    where TConfiguration : ServiceBusProviderConfiguration
{
    private readonly ServiceBusClient _client;
    private readonly ServiceBusSender _sender;
    private readonly IFeederMessageSerializer<TMessage, TConfiguration> _serializer;

    public ServiceBusProvider(TConfiguration configuration, IServiceProvider serviceProvider)
        : this(configuration, serviceProvider, ServiceBusClientFactory.Create(configuration))
    {
    }

    internal ServiceBusProvider(TConfiguration configuration, IServiceProvider serviceProvider, ServiceBusClient client)
        : base(serviceProvider)
    {
        _serializer = serviceProvider.GetRequiredService<IFeederMessageSerializer<TMessage, TConfiguration>>();
        _client = client;
        var entityPath = ServiceBusEntityPath.Parse(configuration.EntityPath);
        _sender = _client.CreateSender(entityPath.EntityName);
    }

    protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
    {
        try
        {
            await _sender.SendMessageAsync(CreateMessage(bytes), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Logger.LogError(exception, "Error occurred while producing an Azure Service Bus message to {EntityPath}.", _sender.EntityPath);
            throw;
        }
    }

    public async Task SendBatchAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ServiceBusMessageBatch? batch = null;
        try
        {
            batch = await _sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
            foreach (var message in messages)
            {
                message.TryAdd("PublishedDateTime", DateTime.UtcNow);
                var serviceBusMessage = CreateMessage(_serializer.SerializeToBytes(message, cancellationToken));
                if (batch.TryAddMessage(serviceBusMessage))
                    continue;

                if (batch.Count == 0)
                    throw new InvalidOperationException("An Azure Service Bus message exceeds the maximum batch size.");

                await _sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
                batch.Dispose();
                batch = await _sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
                if (!batch.TryAddMessage(serviceBusMessage))
                    throw new InvalidOperationException("An Azure Service Bus message exceeds the maximum batch size.");
            }

            if (batch.Count > 0)
                await _sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Logger.LogError(exception, "Error occurred while producing an Azure Service Bus batch to {EntityPath}.", _sender.EntityPath);
            throw;
        }
        finally
        {
            batch?.Dispose();
        }
    }

    private static ServiceBusMessage CreateMessage(byte[] bytes)
    {
        var message = new ServiceBusMessage(new BinaryData(bytes));
        ServiceBusMessagePropagation.Inject(message, Activity.Current?.Context, Baggage.Current);
        return message;
    }

    protected override async ValueTask DisposeManagedResourcesAsync()
    {
        await _sender.DisposeAsync().ConfigureAwait(false);
        await _client.DisposeAsync().ConfigureAwait(false);
    }
}
