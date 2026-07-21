using System.Buffers;
using NATS.Client.Core;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.Feeviders.NATS.SharedKernel
{
    public class JsonNatsSerializer<T>(
        FormatSerializerInvoker formatSerializerInvoker,
        SerializerType serializerType
    ) : INatsSerialize<T>
        where T : notnull
    {
        public void Serialize(IBufferWriter<byte> bufferWriter, T value)
        {
            var array = formatSerializerInvoker(serializerType).SerializeToBytes(value);
            bufferWriter.Write(array);
        }
    }
}
