using Newtonsoft.Json;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
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
                SerializerType.Protobuf => feederMessage.ToProtobufBase64(),
                SerializerType.MessagePack => feederMessage.ToMessagePackBase64(),
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
                SerializerType.Protobuf => feederMessage.ToProtobufBytes(),
                SerializerType.MessagePack => feederMessage.ToMessagePackBytes(),
                _ => throw new ArgumentOutOfRangeException()
            };
    }
}
