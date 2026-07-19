using ThunderPropagator.Feeders.AzureServiceBus;
using ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;
using ThunderPropagator.Providers.DotNet.AzureServiceBus;

namespace ThunderPropagator.ArchTests;

public class AzureServiceBusArchTests
{
    [Theory]
    [InlineData(typeof(ServiceBusFeederConfiguration), "ThunderPropagator.Feeders.AzureServiceBus")]
    [InlineData(typeof(ServiceBusProviderConfiguration), "ThunderPropagator.Providers.DotNet.AzureServiceBus")]
    [InlineData(typeof(IAzureServiceBusFeeviderConfiguration), "ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel")]
    public void PublicTypes_ShouldUseExpectedRootNamespace(Type markerType, string rootNamespace)
    {
        var invalidTypes = markerType.Assembly.ExportedTypes
            .Where(type => type.Namespace is null || !type.Namespace.StartsWith(rootNamespace, StringComparison.Ordinal))
            .ToArray();

        Assert.Empty(invalidTypes);
    }
}
