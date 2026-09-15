using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Feeviders.SharedKernel.Extensions
{
    public static class ThunderPropagatorExtensions
    {
        public static IServiceCollection AddFormatSerializerInvoker(this IServiceCollection services)
        {
            services.TryAddTransient<FormatSerializerInvoker>(serviceProvider =>
            {
                return serializerType =>
                {
                    var deserializers = serviceProvider.GetServices<IFormatSerializer>();
                    var deserializer = deserializers.FirstOrDefault(d => d.SerializerType == serializerType);
                    return deserializer ?? throw new InvalidOperationException($"No IFormatSerializer registered for {serializerType}");
                };
            });

            return services;
        }

        public static IServiceCollection AddFormatDeserializerInvoker(this IServiceCollection services)
        {
            services.TryAddTransient<FormatDeserializerInvoker>(serviceProvider =>
            {
                return serializerType =>
                {
                    var deserializers = serviceProvider.GetServices<IFormatDeserializer>();
                    var deserializer = deserializers.FirstOrDefault(d => d.SerializerType == serializerType);
                    return deserializer ?? throw new InvalidOperationException($"No IFormatDeserializer registered for {serializerType}");
                };
            });

            return services;
        }
    }
}
