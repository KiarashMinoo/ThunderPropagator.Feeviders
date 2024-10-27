using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RapidStreamer.BuildingBlocks.Application;
using RapidStreamer.BuildingBlocks.Application.Objects;

namespace RapidStreamer.Providers.DotNet.SharedKernel
{
    public abstract class AbstractProvider<TFeederMessage, TProviderConfiguration> : DisposableObject,
        IProvider<TFeederMessage>
        where TFeederMessage : FeederMessage
        where TProviderConfiguration : class, IAbstractProviderConfiguration
    {
        private readonly IFeederMessageSerializer<TFeederMessage, TProviderConfiguration> _feederMessageSerializer;
        protected ILogger Logger { get; }

        protected AbstractProvider(IServiceProvider serviceProvider)
        {
            Logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger(GetType());

            _feederMessageSerializer = serviceProvider.GetRequiredService<IFeederMessageSerializer<TFeederMessage, TProviderConfiguration>>();
        }

        public Task ExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default)
        {
            feederMessage.TryAdd("PublishedDateTime", DateTime.UtcNow);
            return InternalExecuteAsync(feederMessage, cancellationToken);
        }

        protected virtual Task InternalExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default)
        {
            return InternalExecuteAsync(_feederMessageSerializer.SerializeToBytes(feederMessage, cancellationToken), cancellationToken);
        }

        protected abstract Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default);
    }
}