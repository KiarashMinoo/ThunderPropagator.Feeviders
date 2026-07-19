using ThunderPropagator.Feeders.GcpPubSub;
using ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;
using ThunderPropagator.Providers.DotNet.GcpPubSub;

namespace ThunderPropagator.ArchTests;

public class GcpPubSubArchTests
{
    [Theory]
    [InlineData(typeof(PubSubFeederConfiguration), "ThunderPropagator.Feeders.GcpPubSub")]
    [InlineData(typeof(PubSubProviderConfiguration), "ThunderPropagator.Providers.DotNet.GcpPubSub")]
    [InlineData(typeof(IGcpPubSubFeeviderConfiguration), "ThunderPropagator.Feeviders.GcpPubSub.SharedKernel")]
    public void PublicTypes_ShouldUseExpectedRootNamespace(Type markerType, string rootNamespace)
    {
        var invalidTypes = markerType.Assembly.ExportedTypes
            .Where(type => type.Namespace is null || !type.Namespace.StartsWith(rootNamespace, StringComparison.Ordinal))
            .ToArray();
        Assert.Empty(invalidTypes);
    }
}
