using System.Runtime.Serialization;
using Confluent.Kafka;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeders.Kafka;

internal sealed class KafkaDeserializer<T>(
    FormatDeserializerInvoker formatDeserializerInvoker,
    IFeeder feeder,
    SerializerType serializerType
) : IAsyncDeserializer<T>
{
    public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
    {
        try
        {
            if (typeof(T) == typeof(Null) && data.Length > 0)
                throw new ArgumentException("The data is null.");

            if (typeof(T) == typeof(Ignore))
                throw new NotSupportedException("Not Supported.");

            return Task.FromResult(formatDeserializerInvoker(serializerType).Deserialize<T>(data.ToArray()) ?? throw new SerializationException());
        }
        catch (Exception exception)
        {
            Console.WriteLine(feeder);
            Console.Error.WriteLine(exception);
            throw;
        }
    }
}
