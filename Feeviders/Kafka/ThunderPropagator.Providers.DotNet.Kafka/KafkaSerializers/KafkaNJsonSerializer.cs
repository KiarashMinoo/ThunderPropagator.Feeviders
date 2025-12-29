using Newtonsoft.Json;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Kafka.KafkaSerializers
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaNJsonSerializer<T> : AbstractKafkaSerializer<T>
        where T : notnull
    {
        public KafkaNJsonSerializer(IProvider provider) : base(provider)
        {
        }

        protected override Task<byte[]> InternalSerializeAsync(T data)
            => Task.FromResult(data.ToNJsonBytes(serializerSettings =>
            {
                serializerSettings.TypeNameHandling = TypeNameHandling.Auto;
                return serializerSettings;
            }));
    }
}