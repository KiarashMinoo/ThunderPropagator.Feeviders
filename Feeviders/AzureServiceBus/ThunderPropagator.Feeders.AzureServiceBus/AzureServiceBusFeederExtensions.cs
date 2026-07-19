using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.AzureServiceBus;

public static class AzureServiceBusFeederExtensions
{
    public static IServiceCollection AddAzureServiceBusFeevider<TChannel, TMessage, TConfiguration>(
        this IServiceCollection services,
        IConfigurationRoot configuration,
        string sectionName)
        where TChannel : class, IChannel
        where TMessage : ServiceBusFeederMessage
        where TConfiguration : ServiceBusFeederConfiguration, new()
    {
        TConfiguration feederConfiguration = new();
        configuration.GetSection(sectionName).Bind(feederConfiguration);
        services.TryAddSingleton(feederConfiguration);
        services.AddChannelFeeder<TChannel, ServiceBusFeeder<TChannel, TMessage, TConfiguration>, TMessage, TConfiguration>();
        return services;
    }

    public static IServiceCollection AddServiceBusFeeder<TChannel, TMessage, TConfiguration>(
        this IServiceCollection services,
        IConfigurationRoot configuration,
        string sectionName)
        where TChannel : class, IChannel
        where TMessage : ServiceBusFeederMessage
        where TConfiguration : ServiceBusFeederConfiguration, new() =>
        services.AddAzureServiceBusFeevider<TChannel, TMessage, TConfiguration>(configuration, sectionName);

    public static IServiceCollection AddAzureServiceBusFeederResolver<TChannel, TMessage, TConfiguration>(this IServiceCollection services)
        where TChannel : class, IChannel
        where TMessage : ServiceBusFeederMessage
        where TConfiguration : ServiceBusFeederConfiguration, new()
    {
        ThunderPropagator.Feeders.SharedKernel.Extensions.AddChannelFeederResolver<TChannel, ServiceBusFeeder<TChannel, TMessage, TConfiguration>, TMessage, TConfiguration>(
            services,
            (serviceProvider, channel, feederConfiguration, feederHandler) =>
                new ServiceBusFeeder<TChannel, TMessage, TConfiguration>(channel, feederConfiguration, feederHandler, serviceProvider));
        return services;
    }

    public static IApplicationBuilder UseAzureServiceBusFeederResolver<TChannel, TMessage, TConfiguration>(
        this IApplicationBuilder app,
        Guid channelKey,
        TConfiguration feederConfiguration)
        where TChannel : class, IChannel
        where TMessage : ServiceBusFeederMessage
        where TConfiguration : ServiceBusFeederConfiguration
    {
        var manager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TMessage, TConfiguration>>();
        manager.UseFeeder(channelKey, feederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);
        return app;
    }
}
