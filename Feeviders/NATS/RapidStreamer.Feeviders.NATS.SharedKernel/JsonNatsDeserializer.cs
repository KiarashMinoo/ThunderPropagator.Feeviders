using System.Buffers;
using NATS.Client.Core;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.BuildingBlocks.Application.Serializations;

namespace RapidStreamer.Feeviders.NATS.SharedKernel
{
    public class JsonNatsDeserializer<T> : INatsDeserialize<T>
    {
        private readonly SerializerType _serializerType;

        public JsonNatsDeserializer(SerializerType serializerType)
        {
            _serializerType = serializerType;
        }

        public T? Deserialize(in ReadOnlySequence<byte> buffer)
        {
            var array = buffer.ToArray();
            return _serializerType switch
            {
                SerializerType.Json => array.FromJsonBytes<T>(),
                SerializerType.NJson => array.FromNJsonBytes<T>(),
                SerializerType.NetJson => array.FromNetJsonBytes<T>(),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }
}