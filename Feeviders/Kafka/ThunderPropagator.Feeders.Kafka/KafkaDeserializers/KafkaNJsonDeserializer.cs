using Newtonsoft.Json;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Runtime.Serialization;

namespace ThunderPropagator.Feeders.Kafka.KafkaDeserializers
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaNJsonDeserializer<T> : AbstractKafkaDeserializer<T>
    {
        public KafkaNJsonDeserializer(IFeeder feeder) : base(feeder)
        {
        }

        protected override Task<T> InternalDeserializeAsync(ReadOnlyMemory<byte> data)
            => Task.FromResult(data.ToArray().FromNJsonBytes<T>(serializerSettings =>
                               {
                                   serializerSettings.TypeNameHandling = TypeNameHandling.Auto;
                                   return serializerSettings;
                               }) ??
                               throw new SerializationException());
    }
}