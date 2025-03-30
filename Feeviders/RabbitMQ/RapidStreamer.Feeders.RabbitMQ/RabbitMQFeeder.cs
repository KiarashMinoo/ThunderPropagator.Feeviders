using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.RabbitMQ.SharedKernel;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;

namespace RapidStreamer.Feeders.RabbitMQ
{
    internal
#if !DEBUG
        sealed
#endif
        class RabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration> : DelegativeFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
        where TChannel : class, RapidStreamer.Application.Channels.IChannel
        where TRabbitMQFeederMessage : RabbitMQFeederMessage
        where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration
    {
        private IChannel? _channel;
        private IConnection? _connection;
        private AsyncEventingBasicConsumer? _consumer;

        private readonly TextMapPropagator _propagator = Propagators.DefaultTextMapPropagator;

        public RabbitMQFeeder(TChannel channel,
            TRabbitMQFeederConfiguration rabbitMqFeederConfiguration,
            IFeederHandler<TChannel, TRabbitMQFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, rabbitMqFeederConfiguration, feederHandler, serviceProvider)
        {
            var applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            new Thread(Start).Start(applicationLifetime.ApplicationStopping);

            HealthName = $"feeder_{nameof(RabbitMQ)}_{rabbitMqFeederConfiguration.Queue}";
            HealthTags = [.. HealthTags, nameof(RabbitMQ), rabbitMqFeederConfiguration.Queue];
        }

        private async void Start(object? state)
        {
            if (state is not CancellationToken cancellationToken)
                cancellationToken = CancellationToken.None;

            (_connection, _channel) = await RabbitMQFeeviderConnectionFactory.InitializeChannelAsync(FeederConfiguration, cancellationToken);

            _consumer = new AsyncEventingBasicConsumer(_channel);

            _consumer.ReceivedAsync += async (_, eventArgs) =>
            {
                try
                {
                    var parentContext = _propagator.Extract(default, eventArgs.BasicProperties, ExtractTraceContextFromBasicProperties);

                    ActivityContext? activityContext = parentContext.ActivityContext;
                    Baggage? baggage = parentContext.Baggage;

                    await ReceiveAsync(eventArgs.Body.ToArray(),
                        activityContext,
                        baggage,
                        new Dictionary<string, object?>
                        {
                            { nameof(eventArgs.Exchange), eventArgs.Exchange },
                            { nameof(eventArgs.ConsumerTag), eventArgs.ConsumerTag },
                            { nameof(eventArgs.DeliveryTag), eventArgs.DeliveryTag },
                            { nameof(eventArgs.RoutingKey), eventArgs.RoutingKey },
                        },
                        cancellationToken);

                    ReportHealth(HealthStatus.Healthy);
                }
                catch (Exception exception)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Logger.LogError(exception, "error has occured while consuming messages on Queue {Queue}.", FeederConfiguration.Queue);
                }
            };

            await _channel.BasicConsumeAsync(FeederConfiguration.Queue, FeederConfiguration.AutoAck, _consumer, cancellationToken: cancellationToken);

            Logger.LogInformation(
                "{Name}/{ChannelName} on Queue {Queue} has configured.",
                GetType().GetTypeInfo().Name,
                Channel.Metadata.ChannelName,
                FeederConfiguration.Queue);
        }

        private IEnumerable<string> ExtractTraceContextFromBasicProperties(IReadOnlyBasicProperties props, string key)
        {
            try
            {
                if (props.Headers?.TryGetValue(key, out var value) == true && value is byte[] bytes)
                    return [Encoding.UTF8.GetString(bytes)];
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to extract trace context: {Message}", ex.Message);
            }

            return [];
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (_channel is not null)
                await _channel.CloseAsync(cancellationToken: cancellationToken);

            if (_connection is not null)
                await _connection.CloseAsync(cancellationToken: cancellationToken);

            await base.StopAsync(cancellationToken);
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await StopAsync();

            if (_channel is not null)
                await _channel.DisposeAsync();

            if (_connection is not null)
                await _connection.DisposeAsync();
        }
    }
}