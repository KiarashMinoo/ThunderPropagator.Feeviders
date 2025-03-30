using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RapidStreamer.Feeviders.RabbitMQ.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.RabbitMQ
{
    internal
#if !DEBUG
        sealed
#endif
        class RabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration> : AbstractProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>
        where TRabbitMQProviderMessage : RabbitMQProviderMessage
        where TRabbitMQProviderConfiguration : RabbitMQProviderConfiguration
    {
        private readonly TRabbitMQProviderConfiguration _rabbitMQProviderConfiguration;
        private IConnection? _connection;
        private IChannel? _channel;

        public RabbitMQProvider(TRabbitMQProviderConfiguration rabbitMQProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _rabbitMQProviderConfiguration = rabbitMQProviderConfiguration;

            var applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            _ = Task.Run(async () => (_connection, _channel) = await RabbitMQFeeviderConnectionFactory.InitializeChannelAsync(_rabbitMQProviderConfiguration, applicationLifetime.ApplicationStopping));
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            if (_channel is null)
                return;

            try
            {
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
                                    Logger.LogError(ex, "Failed to inject trace context.");
                                }
                            });
                }

                await _channel.BasicPublishAsync(_rabbitMQProviderConfiguration.Exchange,
                    _rabbitMQProviderConfiguration.RoutingKey,
                    body: new ReadOnlyMemory<byte>(bytes),
                    basicProperties: channelProperties,
                    cancellationToken: cancellationToken,
                    mandatory: true);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while producing message to queue {Queue}.",
                    _rabbitMQProviderConfiguration.Queue);
                throw;
            }
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