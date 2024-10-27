using Confluent.Kafka;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.Kafka.KafkaSerializers
{
    internal abstract class AbstractKafkaSerializer<T> : IAsyncSerializer<T>
    {
        private readonly IProvider _provider;

        protected AbstractKafkaSerializer(IProvider provider)
        {
            _provider = provider;
        }

        public async Task<byte[]> SerializeAsync(T data, SerializationContext context)
        {
            try
            {
                if (typeof(T) == typeof(Null))
                    return [];

                if (typeof(T) == typeof(Ignore))
                    throw new NotSupportedException("Not Supported.");

                return await InternalSerializeAsync(data);
            }
            catch (Exception exception)
            {
                Console.WriteLine(_provider);
                Console.Error.WriteLine(exception);
                throw;
            }
        }

        protected abstract Task<byte[]> InternalSerializeAsync(T data);
    }
}