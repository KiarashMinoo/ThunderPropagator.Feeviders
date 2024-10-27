using RapidStreamer.BuildingBlocks.Application;

namespace RapidStreamer.Providers.DotNet.SharedKernel
{
    public interface IFeederMessageSerializer<in TFeederMessage, TProviderConfiguration>
        where TFeederMessage : FeederMessage
        where TProviderConfiguration : class, IAbstractProviderConfiguration
    {
        string Serialize(TFeederMessage feederMessage, CancellationToken cancellationToken = default);
        byte[] SerializeToBytes(TFeederMessage feederMessage, CancellationToken cancellationToken = default);
    }
}