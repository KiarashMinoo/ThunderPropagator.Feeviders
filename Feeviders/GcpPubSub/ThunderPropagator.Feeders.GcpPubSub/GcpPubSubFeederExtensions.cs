using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.GcpPubSub;

public static class GcpPubSubFeederExtensions
{
    public static IServiceCollection AddGcpPubSubFeevider<TChannel, TMessage, TConfiguration>(this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
        where TChannel : class, IChannel where TMessage : PubSubFeederMessage where TConfiguration : PubSubFeederConfiguration, new()
    {
        TConfiguration feederConfiguration = new();
        configuration.GetSection(sectionName).Bind(feederConfiguration);
        services.TryAddSingleton(feederConfiguration);
        services.AddChannelFeeder<TChannel, PubSubFeeder<TChannel, TMessage, TConfiguration>, TMessage, TConfiguration>();
        return services;
    }

    public static IServiceCollection AddPubSubFeeder<TChannel, TMessage, TConfiguration>(this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
        where TChannel : class, IChannel where TMessage : PubSubFeederMessage where TConfiguration : PubSubFeederConfiguration, new() =>
        services.AddGcpPubSubFeevider<TChannel, TMessage, TConfiguration>(configuration, sectionName);

    public static IServiceCollection AddGcpPubSubFeederResolver<TChannel, TMessage, TConfiguration>(this IServiceCollection services)
        where TChannel : class, IChannel where TMessage : PubSubFeederMessage where TConfiguration : PubSubFeederConfiguration, new()
    {
        ThunderPropagator.Feeders.SharedKernel.Extensions.AddChannelFeederResolver<TChannel, PubSubFeeder<TChannel, TMessage, TConfiguration>, TMessage, TConfiguration>(services,
            (serviceProvider, channel, feederConfiguration, feederHandler) => new PubSubFeeder<TChannel, TMessage, TConfiguration>(channel, feederConfiguration, feederHandler, serviceProvider));
        return services;
    }

    public static IApplicationBuilder UseGcpPubSubFeederResolver<TChannel, TMessage, TConfiguration>(this IApplicationBuilder app, Guid channelKey, TConfiguration feederConfiguration)
        where TChannel : class, IChannel where TMessage : PubSubFeederMessage where TConfiguration : PubSubFeederConfiguration
    {
        var manager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TMessage, TConfiguration>>();
        manager.UseFeeder(channelKey, feederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);
        return app;
    }
}
