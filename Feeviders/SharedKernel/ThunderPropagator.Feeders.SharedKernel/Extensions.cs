using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Feeders.SharedKernel
{
    internal static class Extensions
    {
        internal static IServiceCollection AddChannelFeederResolver<TChannel, TFeeder, TFeederMessage, TFeederConfiguration>
            (this IServiceCollection services, Func<IServiceProvider, TChannel, TFeederConfiguration, IFeederHandler<TChannel, TFeederMessage>, IFeeder<TChannel>> feederFactory)
            where TChannel : class, IChannel
            where TFeeder : class, IFeeder<TChannel>
            where TFeederMessage : FeederMessage
            where TFeederConfiguration : class, IAbstractFeederConfiguration, new()
            => Infrastructure.Extensions.FeedersExtensions.AddChannelFeederResolver<TChannel, TFeeder, TFeederMessage, TFeederConfiguration>(services, feederFactory);
    }
}