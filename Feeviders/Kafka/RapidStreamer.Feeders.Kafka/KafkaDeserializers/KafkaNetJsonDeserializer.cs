using RapidStreamer.Application.Feeders;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using System.Runtime.Serialization;

namespace RapidStreamer.Feeders.Kafka.KafkaDeserializers
{
    internal
#if !DEBUG
        sealed
#endif
        class KafkaNetJsonDeserializer<T> : AbstractKafkaDeserializer<T>
    {
        public KafkaNetJsonDeserializer(IFeeder feeder) : base(feeder)
        {
        }

        protected override Task<T> InternalDeserializeAsync(ReadOnlyMemory<byte> data)
            => Task.FromResult(data.ToArray().FromNetJsonBytes<T>(serializerSettings =>
                               {
                                   serializerSettings.IncludeTypeInformation = true;
                                   return serializerSettings;
                               }) ??
                               throw new SerializationException());
    }
}