using System.Buffers;
using System.Collections.Immutable;
using System.Reflection;
using DotPulsar;
using DotPulsar.Abstractions;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.BuildingBlocks.Application.Serializations;

namespace RapidStreamer.Feeviders.Pulsar.SharedKernel
{
    public
#if !DEBUG
        sealed
#endif
        class JsonSchema<T> : ISchema<T> where T : notnull
    {
        private readonly SerializerType _serializerType;

        public SchemaInfo SchemaInfo { get; }

        public JsonSchema(SerializerType serializerType)
        {
            _serializerType = serializerType;

            SchemaInfo = new SchemaInfo(typeof(T).GetTypeInfo().Name, [], SchemaType.Json, ImmutableDictionary<string, string>.Empty);
        }

        public T Decode(ReadOnlySequence<byte> bytes, byte[]? schemaVersion = null)
        {
            var array = bytes.ToArray();
            var rtn = _serializerType switch
            {
                SerializerType.Json => array.FromJsonBytes<T>(),
                SerializerType.NJson => array.FromNJsonBytes<T>(),
                SerializerType.NetJson => array.FromNetJsonBytes<T>(),
                _ => throw new ArgumentOutOfRangeException()
            };
            return rtn ?? throw new NullReferenceException();
        }

        public ReadOnlySequence<byte> Encode(T message)
        {
            var array = _serializerType switch
            {
                SerializerType.Json => message.ToJsonBytes(),
                SerializerType.NJson => message.ToNJsonBytes(),
                SerializerType.NetJson => message.ToNetJsonBytes(),
                _ => throw new ArgumentOutOfRangeException()
            };
            return new ReadOnlySequence<byte>(array);
        }
    }
}