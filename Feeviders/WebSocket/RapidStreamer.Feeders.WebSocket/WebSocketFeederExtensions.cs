using System.Net.WebSockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Infrastructure.Extensions;

namespace RapidStreamer.Feeders.WebSocket
{
    public static class WebSocketFeederExtensions
    {
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

            return services;
        }

        public static IServiceCollection AddWebSocketFeederResolver<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TWebSocketFeederMessage : WebSocketFeederMessage
            where TWebSocketFeederConfiguration : WebSocketFeederConfiguration, new()
        {
            services.AddChannelFeederResolver<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>(
                (serviceProvider, channel, webSocketFeederConfiguration, feederHandler) =>
                    new WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>(channel, webSocketFeederConfiguration, feederHandler, serviceProvider));

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