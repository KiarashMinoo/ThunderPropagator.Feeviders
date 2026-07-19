using ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.AzureServiceBus;

public abstract class ServiceBusProviderConfiguration : AbstractProviderConfiguration, IAzureServiceBusFeeviderConfiguration
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
}
