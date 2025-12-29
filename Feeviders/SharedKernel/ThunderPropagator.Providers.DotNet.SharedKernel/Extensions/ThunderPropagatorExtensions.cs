using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.Providers.DotNet.SharedKernel.Extensions
{
    public static class ThunderPropagatorExtensions
    {
        public static IServiceCollection AddChannelProvider<TProvider, TProviderMessage, TProviderConfiguration>
            (this IServiceCollection services, ServiceLifetime serviceLifetime = ServiceLifetime.Scoped)
            where TProvider : AbstractProvider<TProviderMessage, TProviderConfiguration>, IProvider<TProviderMessage>
            where TProviderMessage : FeederMessage
            where TProviderConfiguration : class, IAbstractProviderConfiguration
        {
            services.TryAdd(new ServiceDescriptor(typeof(IProvider<TProviderMessage>), typeof(TProvider), serviceLifetime));
            services.TryAddSingleton<IFeederMessageSerializer<TProviderMessage, TProviderConfiguration>, FeederMessageSerializer<TProviderMessage, TProviderConfiguration>>();

            return services;
        }
    }
}