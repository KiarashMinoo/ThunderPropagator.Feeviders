using ThunderPropagator.Feeviders.Grpc.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Grpc
{
    public abstract class GrpcProviderConfiguration : AbstractGrpcFeevidersConfiguration, IAbstractProviderConfiguration
    {
        public string Topic { get => Get<string>()!; set => Set(value); }
    }
}
