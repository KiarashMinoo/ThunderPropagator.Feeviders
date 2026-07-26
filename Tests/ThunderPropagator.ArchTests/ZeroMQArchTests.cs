using ThunderPropagator.Feeders.ZeroMQ;
using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.ZeroMQ;

namespace ThunderPropagator.ArchTests;

public class ZeroMQArchTests
{
    [Theory]
    [InlineData(typeof(ZeroMqFeederConfiguration), "ThunderPropagator.Feeders.ZeroMQ")]
    [InlineData(typeof(ZeroMqProviderConfiguration), "ThunderPropagator.Providers.DotNet.ZeroMQ")]
    [InlineData(typeof(AbstractZeroMqFeevidersConfiguration), "ThunderPropagator.Feeviders.ZeroMQ.SharedKernel")]
    public void PublicTypes_ShouldUseExpectedRootNamespace(Type markerType, string rootNamespace)
    {
        var invalidTypes = markerType.Assembly.ExportedTypes
            .Where(type => type.Namespace is null || !type.Namespace.StartsWith(rootNamespace, StringComparison.Ordinal))
            .ToArray();

        Assert.Empty(invalidTypes);
    }
}
