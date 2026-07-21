using System.Collections.Concurrent;
using NATS.Client.Core;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

#pragma warning disable CS8714 // The type cannot be used as type parameter in the generic type or method. Nullability of type argument doesn't match 'notnull' constraint.

namespace ThunderPropagator.Feeviders.NATS.SharedKernel
{
    public class JsonNatsSerializerRegistry(
        FormatDeserializerInvoker formatDeserializerInvoker,
        FormatSerializerInvoker formatSerializerInvoker,
        SerializerType serializerType
    ) : INatsSerializerRegistry
    {
        private static readonly ConcurrentDictionary<Type, object> _serializers = new();
        private static readonly ConcurrentDictionary<Type, object> _deserializers = new();

        public INatsSerialize<T> GetSerializer<T>()
            => (INatsSerialize<T>)_serializers.GetOrAdd(typeof(T), _ => new JsonNatsSerializer<T>(formatSerializerInvoker, serializerType));

        public INatsDeserialize<T> GetDeserializer<T>()
            => (INatsDeserialize<T>)_deserializers.GetOrAdd(typeof(T), _ => new JsonNatsDeserializer<T>(formatDeserializerInvoker, serializerType));
    }
}
