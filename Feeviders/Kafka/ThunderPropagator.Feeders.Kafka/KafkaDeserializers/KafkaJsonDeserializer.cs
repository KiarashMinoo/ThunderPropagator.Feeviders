using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Runtime.Serialization;

namespace ThunderPropagator.Feeders.Kafka.KafkaDeserializers
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaJsonDeserializer<T> : AbstractKafkaDeserializer<T>
    {
        public KafkaJsonDeserializer(IFeeder feeder) : base(feeder)
        {
        }

        protected override Task<T> InternalDeserializeAsync(ReadOnlyMemory<byte> data) => Task.FromResult(data.ToArray().FromJsonBytes<T>() ?? throw new SerializationException());
    }
}