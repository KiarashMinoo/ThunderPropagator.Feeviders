using System.Buffers;
using NATS.Client.Core;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeviders.NATS.SharedKernel
{
    public class JsonNatsDeserializer<T>(
        FormatDeserializerInvoker formatDeserializerInvoker,
        SerializerType serializerType
    ) : INatsDeserialize<T>
    {
        public T? Deserialize(in ReadOnlySequence<byte> buffer)
        {
            var array = buffer.ToArray();
            return formatDeserializerInvoker(serializerType).Deserialize<T>(array);
        }
    }
}
