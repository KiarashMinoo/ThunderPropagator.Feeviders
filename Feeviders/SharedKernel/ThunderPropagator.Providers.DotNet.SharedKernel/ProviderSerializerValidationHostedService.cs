using Microsoft.Extensions.Hosting;
using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    internal sealed class ProviderSerializerValidationHostedService<TProviderMessage, TProviderConfiguration>(
        IFeederMessageSerializer<TProviderMessage, TProviderConfiguration> serializer) : IHostedService
        where TProviderMessage : FeederMessage
        where TProviderConfiguration : class, IAbstractProviderConfiguration
    {
        private readonly IFeederMessageSerializer<TProviderMessage, TProviderConfiguration> _serializer = serializer;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _ = _serializer;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
