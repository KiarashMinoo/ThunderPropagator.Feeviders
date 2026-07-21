using Confluent.Kafka;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Kafka;

internal sealed class KafkaSerializer<T>(
    FormatSerializerInvoker formatSerializerInvoker,
    IProvider provider,
    SerializerType serializerType
) : IAsyncSerializer<T>
{
    public Task<byte[]> SerializeAsync(T data, SerializationContext context)
    {
        try
        {
            try
            {
                if (typeof(T) == typeof(Null))
                    return Task.FromResult<byte[]>([]);

                if (typeof(T) == typeof(Ignore))
                    throw new NotSupportedException("Not Supported.");

                return Task.FromResult(formatSerializerInvoker(serializerType).SerializeToBytes(data));
            }
            catch (Exception exception)
            {
                Console.WriteLine(provider);
                Console.Error.WriteLine(exception);
                throw;
            }
        }
        catch (Exception exception1)
        {
            return Task.FromException<byte[]>(exception1);
        }
    }
}
