namespace ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

public interface IAzureServiceBusFeeviderConfiguration
{
    string? ConnectionString { get; set; }
    string? FullyQualifiedNamespace { get; set; }
}
