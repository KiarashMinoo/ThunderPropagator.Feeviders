using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Infrastructure.Extensions;

namespace ThunderPropagator.Feeders.WebApi
{
    public static class WebApiFeederExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.webapi");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.webapi");

        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.webapi.messages.received", "{message}", "Total messages received via WebApi");
        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.webapi.messages.receive.failed", "{message}", "Total WebApi receive failures");
        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.webapi.receive.duration", "ms", "WebApi message receive latency");

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
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>,
                TWebApiFeederMessage,
                TWebApiFeederConfiguration>(services, (serviceProvider, channel, webApiFeederConfiguration, feederHandler) =>
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
                    HttpContext httpContext,
                    CancellationToken cancellationToken)
                => await webApiFeeder.EnqueueAsync(rawMessage,
                    httpContext.Request.Headers.TryGetValue("traceparent", out var traceparentValues) ? traceparentValues.ToString() : null,
                    httpContext.Request.Headers.TryGetValue("tracestate", out var tracestateValues) ? tracestateValues.ToString() : null,
                    cancellationToken);

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