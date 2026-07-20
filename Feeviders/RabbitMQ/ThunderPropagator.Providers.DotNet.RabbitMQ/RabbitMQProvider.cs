using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.RabbitMQ
{
    internal
#if !DEBUG
        sealed
#endif
        partial class RabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration> : AbstractProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>
        where TRabbitMQProviderMessage : RabbitMQProviderMessage
        where TRabbitMQProviderConfiguration : RabbitMQProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4113, Level = LogLevel.Error, Message = "Failed to initialize RabbitMQ channel in background.")]
            public static partial void ChannelInitializationFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4114, Level = LogLevel.Error, Message = "Failed to inject trace context.")]
            public static partial void TraceContextInjectionFailed(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4115, Level = LogLevel.Error, Message = "error has occured while producing message to queue {Queue}.")]
            public static partial void ProduceException(ILogger logger, Exception exception, string queue);
        }

        private readonly TRabbitMQProviderConfiguration _rabbitMQProviderConfiguration;
        private IConnection? _connection;
        private IChannel? _channel;

        public RabbitMQProvider(TRabbitMQProviderConfiguration rabbitMQProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _rabbitMQProviderConfiguration = rabbitMQProviderConfiguration;

            var applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            _ = Task.Run(async () =>
            {
                try
                {
                    var tuple = await RabbitMQFeeviderConnectionFactory.InitializeChannelAsync(_rabbitMQProviderConfiguration, applicationLifetime.ApplicationStopping).ConfigureAwait(false);
                    (_connection, _channel) = tuple;
                }
                catch (Exception ex)
                {
                    // Logging isn't available in constructor context, use provider logger
                    var logger = serviceProvider.GetService<ILogger<RabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>>>();
                    if (logger is not null)
                        Log.ChannelInitializationFailed(logger, ex);
                }
            }, applicationLifetime.ApplicationStopping);
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            try
            {
                var channel = GetReadyChannel(_channel, _rabbitMQProviderConfiguration.Queue);
                var channelProperties = new BasicProperties
                {
                    ContentType = "application/json",
                    DeliveryMode = DeliveryModes.Persistent
                };

                if (Activity.Current?.Context is not null)
                {
                    RabbitMQProviderExtensions.Propagator
                        .Inject(new PropagationContext(Activity.Current.Context, Baggage.Current),
                            channelProperties,
                            (properties, key, value) =>
                            {
                                try
                                {
                                    properties.Headers ??= new Dictionary<string, object?>();
                                    properties.Headers[key] = value;
                                }
                                catch (Exception ex)
                                {
                                    Log.TraceContextInjectionFailed(Logger, ex);
                                }
                            });
                }

                await channel.BasicPublishAsync(_rabbitMQProviderConfiguration.Exchange,
                    _rabbitMQProviderConfiguration.RoutingKey,
                    body: new ReadOnlyMemory<byte>(bytes),
                    basicProperties: channelProperties,
                    cancellationToken: cancellationToken,
                    mandatory: true);
            }
            catch (Exception exception)
            {
                Log.ProduceException(Logger, exception, _rabbitMQProviderConfiguration.Queue);
                throw;
            }
        }

        internal static IChannel GetReadyChannel(IChannel? channel, string queue)
        {
            if (channel is null || !channel.IsOpen)
            {
                throw new InvalidOperationException(
                    $"RabbitMQ provider channel for Queue '{queue}' is not ready. The message was not published.");
            }

            return channel;
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            if (_channel is not null)
                await _channel.CloseAsync();

            if (_connection is not null)
                await _connection.CloseAsync();

            if (_channel is not null)
                await _channel.DisposeAsync();

            if (_connection is not null)
                await _connection.DisposeAsync();
        }
    }
}
