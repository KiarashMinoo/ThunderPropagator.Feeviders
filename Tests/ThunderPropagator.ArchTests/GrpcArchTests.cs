using ThunderPropagator.Feeders.Grpc;
using ThunderPropagator.Feeviders.Grpc.SharedKernel;
using ThunderPropagator.Providers.DotNet.Grpc;

namespace ThunderPropagator.ArchTests;

public class GrpcArchTests
{
    [Theory]
    [InlineData(typeof(GrpcFeederConfiguration), "ThunderPropagator.Feeders.Grpc")]
    [InlineData(typeof(GrpcProviderConfiguration), "ThunderPropagator.Providers.DotNet.Grpc")]
    [InlineData(typeof(AbstractGrpcFeevidersConfiguration), "ThunderPropagator.Feeviders.Grpc.SharedKernel")]
    public void PublicTypes_ShouldUseExpectedRootNamespace(Type markerType, string rootNamespace)
    {
        var invalidTypes = markerType.Assembly.ExportedTypes
            .Where(type => type.Namespace is null || !type.Namespace.StartsWith(rootNamespace, StringComparison.Ordinal))
            .ToArray();

        Assert.Empty(invalidTypes);
    }
}
