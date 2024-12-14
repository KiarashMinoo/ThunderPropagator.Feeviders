using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.Kafka.KafkaSerializers
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaNetJsonSerializer<T> : AbstractKafkaSerializer<T>
        where T : notnull
    {
        public KafkaNetJsonSerializer(IProvider provider) : base(provider)
        {
        }

        protected override Task<byte[]> InternalSerializeAsync(T data)
            => Task.FromResult(data.ToNetJsonBytes(serializerSettings =>
            {
                serializerSettings.IncludeTypeInformation = true;
                return serializerSettings;
            }));
    }
}