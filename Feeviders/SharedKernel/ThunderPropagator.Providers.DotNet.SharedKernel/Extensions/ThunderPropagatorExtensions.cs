using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Providers.DotNet.SharedKernel.Extensions
{
    public static class ThunderPropagatorExtensions
    {
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

        public static IServiceCollection AddChannelProvider<TProvider, TProviderMessage, TProviderConfiguration>
            (this IServiceCollection services, ServiceLifetime serviceLifetime = ServiceLifetime.Scoped)
            where TProvider : AbstractProvider<TProviderMessage, TProviderConfiguration>, IProvider<TProviderMessage>
            where TProviderMessage : FeederMessage
            where TProviderConfiguration : class, IAbstractProviderConfiguration
        {
            services.TryAdd(new ServiceDescriptor(typeof(IProvider<TProviderMessage>), typeof(TProvider), serviceLifetime));
            services.TryAddSingleton<IFeederMessageSerializer<TProviderMessage, TProviderConfiguration>, FeederMessageSerializer<TProviderMessage, TProviderConfiguration>>();
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService,
                ProviderSerializerValidationHostedService<TProviderMessage, TProviderConfiguration>>());

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
