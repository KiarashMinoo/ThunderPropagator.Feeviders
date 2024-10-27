#if DEBUG
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using System.Diagnostics;
#endif
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
        private readonly IConnection _connection;
        private readonly IModel _channel;

        public RabbitMQProvider(TRabbitMQProviderConfiguration rabbitMQProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _rabbitMQProviderConfiguration = rabbitMQProviderConfiguration;

            _connection = RabbitMQFeederConnectionFactory.CreateConnection(_rabbitMQProviderConfiguration);
            _channel = _connection.CreateModel();

            _channel.QueueDeclare(_rabbitMQProviderConfiguration.Queue,
                _rabbitMQProviderConfiguration.Durable,
                _rabbitMQProviderConfiguration.Exclusive,
                _rabbitMQProviderConfiguration.AutoDelete,
                _rabbitMQProviderConfiguration.Arguments);
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            try
            {
                var channelProperties = _channel.CreateBasicProperties();

#if DEBUG
                if (Activity.Current?.Context is not null)
                {
                    RabbitMQProviderExtensions.Propagator.Inject(new PropagationContext(Activity.Current.Context, Baggage.Current), channelProperties, (properties, key, value) =>
                    {
                        try
                        {
                            properties.Headers ??= new Dictionary<string, object>();
                            properties.Headers[key] = value;
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError(ex, "Failed to inject trace context.");
                        }
                    });
                }
#endif

                _channel.BasicPublish(_rabbitMQProviderConfiguration.Exchange, _rabbitMQProviderConfiguration.RoutingKey, body: bytes, basicProperties: channelProperties);

                return Task.CompletedTask;
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while producing message to queue {Queue}.",
                    _rabbitMQProviderConfiguration.Queue);
                throw;
            }
        }

        protected override void DisposeManagedResources()
        {
            _channel.Close();
            _connection.Close();

            _channel.Dispose();
            _connection.Dispose();
        }
    }
}