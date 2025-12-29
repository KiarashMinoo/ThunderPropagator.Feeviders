using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    public interface IProvider : IDisposable;

    public interface IProvider<in TFeederMessage> : IProvider
        where TFeederMessage : FeederMessage
    {
        Task ExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default);
    }
}