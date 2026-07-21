using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.GcpPubSub;

public static class GcpPubSubProviderExtensions
{
    public static IServiceCollection AddGcpPubSubFeevider<TMessage, TConfiguration>(this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
        where TMessage : PubSubProviderMessage where TConfiguration : PubSubProviderConfiguration, new()
    {
        TConfiguration providerConfiguration = new();
        configuration.GetSection(sectionName).Bind(providerConfiguration);
        services.TryAddSingleton(providerConfiguration);
        services.AddChannelProvider<PubSubProvider<TMessage, TConfiguration>, TMessage, TConfiguration>();
        services.AddFormatSerializerInvoker();
        services.AddFormatDeserializerInvoker();
        return services;
    }

    public static IServiceCollection AddPubSubProvider<TMessage, TConfiguration>(this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
        where TMessage : PubSubProviderMessage where TConfiguration : PubSubProviderConfiguration, new() =>
        services.AddGcpPubSubFeevider<TMessage, TConfiguration>(configuration, sectionName);
}
