using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Infrastructure.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Feeders.WebSocket
{
    public static class WebSocketFeederExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.websocket");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.websocket");

        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.websocket.messages.received",
            description: "Number of WebSocket messages successfully received.");

        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.websocket.messages.receive.failed",
            description: "Number of WebSocket messages that failed to be received/processed.");

        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.websocket.receive.duration",
            unit: "ms",
            description: "Duration of WebSocket message receive processing.");

        public static IServiceCollection AddWebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TWebSocketFeederMessage : WebSocketFeederMessage
            where TWebSocketFeederConfiguration : WebSocketFeederConfiguration, new()
        {
            TWebSocketFeederConfiguration webSocketFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(webSocketFeederConfiguration);
            services.TryAddSingleton(webSocketFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>,
                TWebSocketFeederMessage,
                TWebSocketFeederConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }


        public static IServiceCollection AddWebSocketFeederResolver<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TWebSocketFeederMessage : WebSocketFeederMessage
            where TWebSocketFeederConfiguration : WebSocketFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>,
                TWebSocketFeederMessage,
                TWebSocketFeederConfiguration>(services, (serviceProvider, channel, webSocketFeederConfiguration, feederHandler) =>
                new WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>(channel, webSocketFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IApplicationBuilder UseWebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>(this IApplicationBuilder applicationBuilder)
            where TChannel : class, IChannel
            where TWebSocketFeederMessage : WebSocketFeederMessage
            where TWebSocketFeederConfiguration : WebSocketFeederConfiguration
        {
            var webSocketConfiguration = applicationBuilder.ApplicationServices.GetRequiredService<TWebSocketFeederConfiguration>();
            applicationBuilder.UseWebSockets(webSocketConfiguration);

            applicationBuilder.Use(async (context, next) =>
            {
                if (!context.Request.Path.Equals(webSocketConfiguration.Path, StringComparison.OrdinalIgnoreCase))
                {
                    await next(context);
                    return;
                }

                if (!webSocketConfiguration.IsEnabled)
                {
                    context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                    await context.Response.WriteAsync("WebSocket feeder is disabled.", context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                if (context.WebSockets.IsWebSocketRequest)
                {
                    using var webSocket = await context.WebSockets.AcceptWebSocketAsync();

                    var webSocketFeeder = applicationBuilder.ApplicationServices.GetRequiredService<WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>>();

                    while (!context.RequestAborted.IsCancellationRequested)
                    {
                        List<byte> message = [];

                        var buffer = new ArraySegment<byte>(new byte[webSocketConfiguration.BufferSize]);
                        WebSocketReceiveResult result;

                        using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
                        cancellationTokenSource.CancelAfter(TimeSpan.FromHours(24));

                        do
                        {
                            result = await webSocket.ReceiveAsync(buffer, cancellationTokenSource.Token);
                            message.AddRange(buffer.ToArray());

                            if (message.Count > webSocketConfiguration.MaxRequestSize)
                                throw new InvalidOperationException("the message length has exceeded maximum allowed size");
                        } while (!result.EndOfMessage);

                        if (result.MessageType == WebSocketMessageType.Close)
                            break;

                        await webSocketFeeder.EnqueueAsync(message.ToArray(), cancellationTokenSource.Token);
                    }
                }
                else
                {
                    context.Response.StatusCode = StatusCodes.Status400BadRequest;
                }
            });

            return applicationBuilder;
        }

        public static IApplicationBuilder UseWebSocketFeederResolver<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TWebSocketFeederConfiguration webSocketFeederConfiguration)
            where TChannel : class, IChannel
            where TWebSocketFeederMessage : WebSocketFeederMessage
            where TWebSocketFeederConfiguration : WebSocketFeederConfiguration
        {
            var webSocketFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>>();

            webSocketFeederManager.UseFeeder(channelKey, webSocketFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}
