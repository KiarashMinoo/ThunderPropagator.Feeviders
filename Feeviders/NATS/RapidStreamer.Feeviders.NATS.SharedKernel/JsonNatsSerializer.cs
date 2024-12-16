using System.Buffers;
using NATS.Client.Core;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.BuildingBlocks.Application.Serializations;

namespace RapidStreamer.Feeviders.NATS.SharedKernel
{
    public class JsonNatsSerializer<T> : INatsSerialize<T> where T : notnull
    {
        private readonly SerializerType _serializerType;

        public JsonNatsSerializer(SerializerType serializerType)
        {
            _serializerType = serializerType;
        }

        public void Serialize(IBufferWriter<byte> bufferWriter, T value)
        {
            var array = _serializerType switch
            {
                SerializerType.Json => value.ToJsonBytes(),
                SerializerType.NJson => value.ToNJsonBytes(),
                SerializerType.NetJson => value.ToNetJsonBytes(),
                _ => throw new ArgumentOutOfRangeException()
            };

            bufferWriter.Write(array);
        }
    }
}