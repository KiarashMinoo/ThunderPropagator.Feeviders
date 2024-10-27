using Newtonsoft.Json;
using RapidStreamer.BuildingBlocks.Application;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.BuildingBlocks.Application.Serializations;

namespace RapidStreamer.Providers.DotNet.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class FeederMessageSerializer<TProviderMessage, TProviderConfiguration> : IFeederMessageSerializer<TProviderMessage, TProviderConfiguration>
        where TProviderMessage : FeederMessage
        where TProviderConfiguration : class, IAbstractProviderConfiguration
    {
        private readonly TProviderConfiguration _feederConfiguration;

        public FeederMessageSerializer(TProviderConfiguration feederConfiguration) => _feederConfiguration = feederConfiguration;

        public string Serialize(TProviderMessage feederMessage, CancellationToken cancellationToken = default)
            => _feederConfiguration.SerializerType switch
            {
                SerializerType.Json => feederMessage.ToJson(),
                SerializerType.NJson => feederMessage.ToNJson(serializerSettings =>
                {
                    serializerSettings.TypeNameHandling = TypeNameHandling.Auto;
                    return serializerSettings;
                }),
                _ => throw new ArgumentOutOfRangeException()
            };

        public byte[] SerializeToBytes(TProviderMessage feederMessage, CancellationToken cancellationToken = default)
            => _feederConfiguration.SerializerType switch
            {
                SerializerType.Json => feederMessage.ToJsonBytes(),
                SerializerType.NJson => feederMessage.ToNJsonBytes(serializerSettings =>
                {
                    serializerSettings.TypeNameHandling = TypeNameHandling.Auto;
                    return serializerSettings;
                }),
                _ => throw new ArgumentOutOfRangeException()
            };
    }
}