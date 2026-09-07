using ThunderPropagator.Feeviders.Grpc.SharedKernel;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Grpc
{
    public abstract class GrpcProviderConfiguration : AbstractGrpcFeevidersConfiguration, IAbstractProviderConfiguration
    {
        public string Topic { get => Get<string>()!; set => Set(value); }

        public Guid Id { get => Get(Guid.NewGuid()); set => Set(value); }

        /// <summary>Opt-in transactional Outbox configuration for this Provider. Disabled by default.</summary>
        public OutboxOptions Outbox { get => Get(new OutboxOptions()); set => Set(value); }
    }
}
