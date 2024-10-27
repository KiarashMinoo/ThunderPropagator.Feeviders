using Confluent.Kafka;
using RapidStreamer.Application.Feeders;

namespace RapidStreamer.Feeders.Kafka.KafkaDeserializers
{
    internal abstract class AbstractKafkaDeserializer<T> : IAsyncDeserializer<T>
    {
        private readonly IFeeder _feeder;

        protected AbstractKafkaDeserializer(IFeeder feeder)
        {
            _feeder = feeder;
        }

        public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            try
            {
                if (typeof(T) == typeof(Null) && data.Length > 0)
                    throw new ArgumentException("The data is null.");

                if (typeof(T) == typeof(Ignore))
                    throw new NotSupportedException("Not Supported.");

                return InternalDeserializeAsync(data);
            }
            catch (Exception exception)
            {
                Console.WriteLine(_feeder);
                Console.Error.WriteLine(exception);
                throw;
            }
        }

        protected abstract Task<T> InternalDeserializeAsync(ReadOnlyMemory<byte> data);
    }
}