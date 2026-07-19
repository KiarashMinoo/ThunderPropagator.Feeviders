using Azure.Identity;
using Azure.Messaging.ServiceBus;

namespace ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

internal static class ServiceBusClientFactory
{
    public static ServiceBusClient Create(IAzureServiceBusFeeviderConfiguration configuration)
    {
        if (!string.IsNullOrWhiteSpace(configuration.ConnectionString))
            return new ServiceBusClient(configuration.ConnectionString);

        ArgumentException.ThrowIfNullOrWhiteSpace(configuration.FullyQualifiedNamespace);
        return new ServiceBusClient(configuration.FullyQualifiedNamespace, new DefaultAzureCredential());
    }
}
