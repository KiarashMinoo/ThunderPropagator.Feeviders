using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Infrastructure.Extensions;

namespace RapidStreamer.Feeders.WebApi
{
    public static class WebApiFeederExtensions
    {
        public static IServiceCollection AddWebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TWebApiFeederMessage : WebApiFeederMessage
            where TWebApiFeederConfiguration : WebApiFeederConfiguration, new()
        {
            TWebApiFeederConfiguration webApiFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(webApiFeederConfiguration);
            services.TryAddSingleton(webApiFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>,
                TWebApiFeederMessage,
                TWebApiFeederConfiguration>();

            return services;
        }

        public static IServiceCollection AddWebApiFeederResolver<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TWebApiFeederMessage : WebApiFeederMessage
            where TWebApiFeederConfiguration : WebApiFeederConfiguration, new()
        {
            services.AddChannelFeederResolver<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>(
                (serviceProvider, channel, webApiFeederConfiguration, feederHandler) =>
                    new WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>(channel, webApiFeederConfiguration, feederHandler, serviceProvider));

            return services;
        }

        public static IEndpointRouteBuilder UseWebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>(this IEndpointRouteBuilder endpointRouteBuilder)
            where TChannel : class, IChannel
            where TWebApiFeederMessage : WebApiFeederMessage
            where TWebApiFeederConfiguration : WebApiFeederConfiguration
        {
            Delegate receiveMessage = async ([FromServices] WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration> webApiFeeder,
                    [FromBody] string rawMessage,
                    CancellationToken cancellationToken)
                => await webApiFeeder.EnqueueAsync(rawMessage, cancellationToken);

            var webApiFeederConfiguration = endpointRouteBuilder.ServiceProvider.GetRequiredService<TWebApiFeederConfiguration>();

            endpointRouteBuilder.MapPost(webApiFeederConfiguration.Path, receiveMessage);

            return endpointRouteBuilder;
        }

        public static IApplicationBuilder UseWebApiFeederResolver<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TWebApiFeederConfiguration webApiFeederConfiguration)
            where TChannel : class, IChannel
            where TWebApiFeederMessage : WebApiFeederMessage
            where TWebApiFeederConfiguration : WebApiFeederConfiguration
        {
            var webApiFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>>();

            webApiFeederManager.UseFeeder(channelKey, webApiFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}