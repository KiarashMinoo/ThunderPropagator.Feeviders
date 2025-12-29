using System.Collections.Concurrent;
using NATS.Client.Core;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
#pragma warning disable CS8714 // The type cannot be used as type parameter in the generic type or method. Nullability of type argument doesn't match 'notnull' constraint.

namespace ThunderPropagator.Feeviders.NATS.SharedKernel
{
    public class JsonNatsSerializerRegistry : INatsSerializerRegistry
    {
        private static ConcurrentDictionary<Type, object> _serializers = new();
        private static ConcurrentDictionary<Type, object> _deserializers = new();
        private readonly SerializerType _serializerType;

        public JsonNatsSerializerRegistry(SerializerType serializerType) => _serializerType = serializerType;

        public INatsSerialize<T> GetSerializer<T>()
            => (INatsSerialize<T>)_serializers.GetOrAdd(typeof(T), _ => new JsonNatsSerializer<T>(_serializerType));

        public INatsDeserialize<T> GetDeserializer<T>()
            => (INatsDeserialize<T>)_deserializers.GetOrAdd(typeof(T), _ => new JsonNatsDeserializer<T>(_serializerType));
    }
}