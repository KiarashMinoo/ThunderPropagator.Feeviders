using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;
using System.Reflection;
using System.Text;
using OpenTelemetry;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.Feeders.RabbitMQ
{
    internal
#if !DEBUG
        sealed
#endif
        class RabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration> : DelegativeFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
        where TChannel : class, ThunderPropagator.Application.Channels.IChannel
        where TRabbitMQFeederMessage : RabbitMQFeederMessage
        where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration
    {
        private IChannel? _channel;
        private IConnection? _connection;
        private AsyncEventingBasicConsumer? _consumer;
        private readonly InFlightMessageTracker _inFlightMessages = new();
        private readonly CancellationTokenSource _receiveCancellation = new();

        private readonly TextMapPropagator _propagator = Propagators.DefaultTextMapPropagator;

        public RabbitMQFeeder(TChannel channel,
            TRabbitMQFeederConfiguration rabbitMqFeederConfiguration,
            IFeederHandler<TChannel, TRabbitMQFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, rabbitMqFeederConfiguration, feederHandler, serviceProvider)
        {
            HealthName = $"feeder_{nameof(RabbitMQ)}_{rabbitMqFeederConfiguration.Queue}";
            HealthTags = [.. HealthTags, nameof(RabbitMQ), rabbitMqFeederConfiguration.Queue];
        }

        protected override async Task StartAsync(CancellationToken cancellationToken = default)
        {
            (_connection, _channel) = await RabbitMQFeeviderConnectionFactory.InitializeChannelAsync(FeederConfiguration, cancellationToken).ConfigureAwait(false);

            _consumer = new AsyncEventingBasicConsumer(_channel);

            _consumer.ReceivedAsync += async (_, eventArgs) =>
            {
                if (!_inFlightMessages.TryBegin())
                    return;

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
                        _receiveCancellation.Token).ConfigureAwait(false);

                    ReportHealth(HealthStatus.Healthy);
                }
                catch (Exception exception)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Logger.LogError(exception, "error has occured while consuming messages on Queue {Queue}.", FeederConfiguration.Queue);
                }
                finally
                {
                    _inFlightMessages.Complete();
                }
            };

            await _channel.BasicConsumeAsync(FeederConfiguration.Queue, FeederConfiguration.AutoAck, _consumer, cancellationToken: cancellationToken).ConfigureAwait(false);

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
            await _inFlightMessages.DrainAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            await _receiveCancellation.CancelAsync().ConfigureAwait(false);

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

            _receiveCancellation.Dispose();
        }
    }
}
