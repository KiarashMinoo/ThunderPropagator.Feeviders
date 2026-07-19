namespace ThunderPropagator.Providers.DotNet.AzureServiceBus;

public interface IServiceBusBatchProvider<in TMessage>
{
    Task SendBatchAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default);
}
