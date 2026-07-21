using System.Buffers;
using System.Collections.Immutable;
using System.Reflection;
using DotPulsar;
using DotPulsar.Abstractions;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeviders.Pulsar.SharedKernel
{
    public
#if !DEBUG
        sealed
#endif
        class JsonSchema<T>(
            FormatDeserializerInvoker formatDeserializerInvoker,
            FormatSerializerInvoker formatSerializerInvoker,
            SerializerType serializerType
        ) : ISchema<T>
        where T : notnull
    {
        public SchemaInfo SchemaInfo { get; } = new(typeof(T).GetTypeInfo().Name, [], SchemaType.Json, ImmutableDictionary<string, string>.Empty);

        public T Decode(ReadOnlySequence<byte> bytes, byte[]? schemaVersion = null)
        {
            return formatDeserializerInvoker(serializerType).Deserialize<T>(bytes.ToArray()) ?? throw new NullReferenceException();
        }

        public ReadOnlySequence<byte> Encode(T message)
        {
            var array = formatSerializerInvoker(serializerType).SerializeToBytes(message);
            return new ReadOnlySequence<byte>(array);
        }
    }
}
