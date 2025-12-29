using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    public interface IFeederMessageSerializer<in TFeederMessage, TProviderConfiguration>
        where TFeederMessage : FeederMessage
        where TProviderConfiguration : class, IAbstractProviderConfiguration
    {
        string Serialize(TFeederMessage feederMessage, CancellationToken cancellationToken = default);
        byte[] SerializeToBytes(TFeederMessage feederMessage, CancellationToken cancellationToken = default);
    }
}