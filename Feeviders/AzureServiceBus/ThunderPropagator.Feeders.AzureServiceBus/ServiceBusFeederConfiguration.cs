using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

namespace ThunderPropagator.Feeders.AzureServiceBus;

public abstract class ServiceBusFeederConfiguration : AbstractFeederConfiguration, IAzureServiceBusFeeviderConfiguration
{
    public string? ConnectionString
    {
        get => Get<string>();
        set => Set(value);
    }

    public string? FullyQualifiedNamespace
    {
        get => Get<string>();
        set => Set(value);
    }

    public string EntityPath
    {
        get => Get<string>()!;
        set => Set(value);
    }

    public int MaxConcurrentCalls
    {
        get => Get(1);
        set => Set(value);
    }

    public int MaxDeliveryCount
    {
        get => Get(10);
        set => Set(value);
    }
}
