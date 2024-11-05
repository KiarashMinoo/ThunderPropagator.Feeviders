using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.RabbitMQ.SharedKernel;
using System.Reflection;
using System.Text;

namespace RapidStreamer.Feeders.RabbitMQ
{
    internal
#if !DEBUG
        sealed
#endif
        class RabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration> : DelegativeFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
        where TChannel : class, IChannel
        where TRabbitMQFeederMessage : RabbitMQFeederMessage
        where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration
    {
        private readonly IModel _channel;
        private readonly IConnection _connection;
        private readonly AsyncEventingBasicConsumer _consumer;

        private readonly TextMapPropagator _propagator = Propagators.DefaultTextMapPropagator;

        public RabbitMQFeeder(TChannel channel,
            TRabbitMQFeederConfiguration rabbitMqFeederConfiguration,
            IFeederHandler<TChannel, TRabbitMQFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, rabbitMqFeederConfiguration, feederHandler, serviceProvider)
        {
            _connection = RabbitMQFeederConnectionFactory.CreateConnection(rabbitMqFeederConfiguration);
            _channel = _connection.CreateModel();

            _channel.QueueDeclare(rabbitMqFeederConfiguration.Queue,
                rabbitMqFeederConfiguration.Durable,
                rabbitMqFeederConfiguration.Exclusive,
                rabbitMqFeederConfiguration.AutoDelete,
                rabbitMqFeederConfiguration.Arguments);

            _consumer = new AsyncEventingBasicConsumer(_channel);

            _consumer.Received += async (_, eventArgs) =>
            {
                try
                {
#if DEBUG
                    var parentContext = _propagator.Extract(default, eventArgs.BasicProperties, ExtractTraceContextFromBasicProperties);
                    await ReceiveAsync(eventArgs.Body.ToArray(),
                        parentContext.ActivityContext,
                        parentContext.Baggage,
                        new Dictionary<string, object?>
                        {
                            { nameof(eventArgs.Exchange), eventArgs.Exchange },
                            { nameof(eventArgs.ConsumerTag), eventArgs.ConsumerTag },
                            { nameof(eventArgs.DeliveryTag), eventArgs.DeliveryTag },
                            { nameof(eventArgs.RoutingKey), eventArgs.RoutingKey },
                        });
#else
                    await ReceiveAsync(eventArgs.Body.ToArray(),
                        arguments: new Dictionary<string, object?>
                        {
                            { nameof(eventArgs.Exchange), eventArgs.Exchange },
                            { nameof(eventArgs.ConsumerTag), eventArgs.ConsumerTag },
                            { nameof(eventArgs.DeliveryTag), eventArgs.DeliveryTag },
                            { nameof(eventArgs.RoutingKey), eventArgs.RoutingKey },
                        });
#endif

                    ReportHealth(HealthStatus.Healthy);
                }
                catch (Exception exception)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Logger.LogError(exception, "error has occured while consuming messages on Queue {Queue}.", rabbitMqFeederConfiguration.Queue);
                }
            };

            _channel.BasicConsume(rabbitMqFeederConfiguration.Queue,
                rabbitMqFeederConfiguration.AutoAck,
                _consumer);

            Logger.LogInformation("{Name}/{ChannelName} on Queue {Queue} has configured.", GetType().GetTypeInfo().Name, channel.Metadata.ChannelName,
                rabbitMqFeederConfiguration.Queue);

            HealthName = $"feeder_{nameof(RabbitMQ)}_{rabbitMqFeederConfiguration.Queue}";
            HealthTags = [.. HealthTags, nameof(RabbitMQ), rabbitMqFeederConfiguration.Queue];
        }

        private IEnumerable<string> ExtractTraceContextFromBasicProperties(IBasicProperties props, string key)
        {
            try
            {
                if (props.Headers.TryGetValue(key, out var value))
                {
                    var bytes = value as byte[];
                    return new[] { Encoding.UTF8.GetString(bytes!) };
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to extract trace context: {Message}", ex.Message);
            }

            return [];
        }

        protected override Task StopAsync(CancellationToken cancellationToken = default)
        {
            _channel.Close();
            _connection.Close();

            return base.StopAsync(cancellationToken);
        }

        protected override void DisposeManagedResources()
        {
            _channel.Dispose();
            _connection.Dispose();
        }
    }
}