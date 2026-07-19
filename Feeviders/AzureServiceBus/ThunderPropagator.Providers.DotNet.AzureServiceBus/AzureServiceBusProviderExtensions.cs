using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.AzureServiceBus;

public static class AzureServiceBusProviderExtensions
{
    public static IServiceCollection AddAzureServiceBusFeevider<TMessage, TConfiguration>(
        this IServiceCollection services,
        IConfigurationRoot configuration,
        string sectionName)
        where TMessage : ServiceBusProviderMessage
        where TConfiguration : ServiceBusProviderConfiguration, new()
    {
        TConfiguration providerConfiguration = new();
        configuration.GetSection(sectionName).Bind(providerConfiguration);
        services.TryAddSingleton(providerConfiguration);
        services.AddChannelProvider<ServiceBusProvider<TMessage, TConfiguration>, TMessage, TConfiguration>();
        services.TryAddScoped<IServiceBusBatchProvider<TMessage>>(serviceProvider =>
            (ServiceBusProvider<TMessage, TConfiguration>)serviceProvider.GetRequiredService<IProvider<TMessage>>());
        return services;
    }

    public static IServiceCollection AddServiceBusProvider<TMessage, TConfiguration>(
        this IServiceCollection services,
        IConfigurationRoot configuration,
        string sectionName)
        where TMessage : ServiceBusProviderMessage
        where TConfiguration : ServiceBusProviderConfiguration, new() =>
        services.AddAzureServiceBusFeevider<TMessage, TConfiguration>(configuration, sectionName);
}
