using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Feeders.SharedKernel
{
    public static class Extensions
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

        internal static IServiceCollection AddChannelFeederResolver<TChannel, TFeeder, TFeederMessage, TFeederConfiguration>
            (this IServiceCollection services, Func<IServiceProvider, TChannel, TFeederConfiguration, IFeederHandler<TChannel, TFeederMessage>, IFeeder<TChannel>> feederFactory)
            where TChannel : class, IChannel
            where TFeeder : class, IFeeder<TChannel>
            where TFeederMessage : FeederMessage
            where TFeederConfiguration : class, IAbstractFeederConfiguration, new()
            => Infrastructure.Extensions.FeedersExtensions.AddChannelFeederResolver<TChannel, TFeeder, TFeederMessage, TFeederConfiguration>(services, feederFactory);
    }
}
