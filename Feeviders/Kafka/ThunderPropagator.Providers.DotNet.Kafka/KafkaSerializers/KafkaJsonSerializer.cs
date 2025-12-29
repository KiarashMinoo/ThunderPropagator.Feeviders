using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using System.Text.Json;

namespace ThunderPropagator.Providers.DotNet.Kafka.KafkaSerializers
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaJsonSerializer<T> : AbstractKafkaSerializer<T>
        where T : notnull
    {
        private readonly JsonSerializerOptions? _serializerOptions;

        public KafkaJsonSerializer(IProvider provider, JsonSerializerOptions? serializerOptions = null) : base(provider) => _serializerOptions = serializerOptions;

        protected override Task<byte[]> InternalSerializeAsync(T data) => Task.FromResult(data.ToJsonBytes(serializerOptions => _serializerOptions ?? serializerOptions));
    }
}