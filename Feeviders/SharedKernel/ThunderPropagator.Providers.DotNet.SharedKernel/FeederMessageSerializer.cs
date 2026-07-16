using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class FeederMessageSerializer<TProviderMessage, TProviderConfiguration> : IFeederMessageSerializer<TProviderMessage, TProviderConfiguration>
        where TProviderMessage : FeederMessage
        where TProviderConfiguration : class, IAbstractProviderConfiguration
    {
        private readonly IFormatSerializer _serializer;

        public FeederMessageSerializer(TProviderConfiguration feederConfiguration, IFormatSerializerRegistry serializerRegistry)
        {
            try
            {
                _serializer = serializerRegistry.GetSerializer(feederConfiguration.SerializerType);
            }
            catch (InvalidOperationException exception)
            {
                throw new InvalidOperationException(
                    $"No format serializer is registered for SerializerType '{feederConfiguration.SerializerType}' configured by '{typeof(TProviderConfiguration).Name}'. Register a matching IFormatSerializer before adding the provider.",
                    exception);
            }
        }

        public string Serialize(TProviderMessage feederMessage, CancellationToken cancellationToken = default)
            => _serializer.Serialize(feederMessage);

        public byte[] SerializeToBytes(TProviderMessage feederMessage, CancellationToken cancellationToken = default)
            => _serializer.SerializeToBytes(feederMessage);
    }
}
