using System.Diagnostics;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;

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
        private readonly CancellationTokenSource _lifetimeCancellation = new();
        private readonly SemaphoreSlim _reconnectLock = new(1, 1);
        private int _stopping;

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
            _ = RabbitMQReconnectDelay.Calculate(
                FeederConfiguration.ReconnectInitialDelay,
                FeederConfiguration.ReconnectMaxDelay,
                1);

            await ConnectAndConsumeAsync(cancellationToken).ConfigureAwait(false);

            Logger.LogInformation(
                "{Name}/{ChannelName} on Queue {Queue} has configured.",
                GetType().GetTypeInfo().Name,
                Channel.Metadata.ChannelName,
                FeederConfiguration.Queue);
        }

        private async Task ConnectAndConsumeAsync(CancellationToken cancellationToken)
        {
            var (connection, channel) = await RabbitMQFeeviderConnectionFactory
                .InitializeChannelAsync(FeederConfiguration, cancellationToken)
                .ConfigureAwait(false);
            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += HandleReceivedAsync;
            connection.ConnectionShutdownAsync += HandleShutdownAsync;
            channel.ChannelShutdownAsync += HandleShutdownAsync;

            try
            {
                await channel.BasicConsumeAsync(
                    FeederConfiguration.Queue,
                    FeederConfiguration.AutoAck,
                    consumer,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                _connection = connection;
                _channel = channel;
                _consumer = consumer;
            }
            catch
            {
                consumer.ReceivedAsync -= HandleReceivedAsync;
                connection.ConnectionShutdownAsync -= HandleShutdownAsync;
                channel.ChannelShutdownAsync -= HandleShutdownAsync;
                await DisposeBrokerResourcesAsync(connection, channel, false, CancellationToken.None).ConfigureAwait(false);
                throw;
            }
        }

        private async Task HandleReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
        {
            var deliveryChannel = ((AsyncEventingBasicConsumer)sender).Channel;

            if (!_inFlightMessages.TryBegin())
            {
                await RabbitMQDeliveryAcknowledger.NegativeAcknowledgeAsync(
                    deliveryChannel,
                    eventArgs.DeliveryTag,
                    FeederConfiguration.AutoAck,
                    true,
                    CancellationToken.None).ConfigureAwait(false);
                return;
            }

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

                await RabbitMQDeliveryAcknowledger.AcknowledgeAsync(
                    deliveryChannel,
                    eventArgs.DeliveryTag,
                    FeederConfiguration.AutoAck,
                    _receiveCancellation.Token).ConfigureAwait(false);

                ReportHealth(HealthStatus.Healthy);
            }
            catch (Exception exception)
            {
                try
                {
                    await RabbitMQDeliveryAcknowledger.NegativeAcknowledgeAsync(
                        deliveryChannel,
                        eventArgs.DeliveryTag,
                        FeederConfiguration.AutoAck,
                        exception is OperationCanceledException || FeederConfiguration.RequeueOnFailure,
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception negativeAcknowledgeException)
                {
                    Logger.LogError(negativeAcknowledgeException,
                        "Failed to negatively acknowledge Delivery {DeliveryTag} on Queue {Queue}.",
                        eventArgs.DeliveryTag,
                        FeederConfiguration.Queue);
                }

                ReportHealth(HealthStatus.Unhealthy, exception);

                Logger.LogError(exception, "error has occured while consuming messages on Queue {Queue}.", FeederConfiguration.Queue);
            }
            finally
            {
                _inFlightMessages.Complete();
            }
        }

        private Task HandleShutdownAsync(object sender, ShutdownEventArgs eventArgs)
        {
            if (Volatile.Read(ref _stopping) != 0 ||
                (!ReferenceEquals(sender, _connection) && !ReferenceEquals(sender, _channel)))
                return Task.CompletedTask;

            ReportHealth(HealthStatus.Unhealthy);
            Logger.LogWarning(
                "RabbitMQ {Component} for Queue {Queue} shut down: {ReplyText}. Reconnection will be attempted.",
                ReferenceEquals(sender, _connection) ? "connection" : "channel",
                FeederConfiguration.Queue,
                eventArgs.ReplyText);

            _ = Task.Run(() => ReconnectAsync(sender), CancellationToken.None);

            return Task.CompletedTask;
        }

        private async Task ReconnectAsync(object shutdownSource)
        {
            try
            {
                await _reconnectLock.WaitAsync(_lifetimeCancellation.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                if (Volatile.Read(ref _stopping) != 0 ||
                    (!ReferenceEquals(shutdownSource, _connection) && !ReferenceEquals(shutdownSource, _channel)))
                    return;

                await ReleaseCurrentBrokerResourcesAsync(false, CancellationToken.None).ConfigureAwait(false);

                var attempt = 1;
                while (!_lifetimeCancellation.IsCancellationRequested)
                {
                    try
                    {
                        await ConnectAndConsumeAsync(_lifetimeCancellation.Token).ConfigureAwait(false);
                        ReportHealth(HealthStatus.Healthy);
                        Logger.LogInformation(
                            "RabbitMQ feeder on Queue {Queue} reconnected after {AttemptCount} attempt(s).",
                            FeederConfiguration.Queue,
                            attempt);
                        return;
                    }
                    catch (OperationCanceledException) when (_lifetimeCancellation.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        ReportHealth(HealthStatus.Unhealthy, exception);
                        var delay = RabbitMQReconnectDelay.Calculate(
                            FeederConfiguration.ReconnectInitialDelay,
                            FeederConfiguration.ReconnectMaxDelay,
                            attempt);

                        Logger.LogWarning(exception,
                            "RabbitMQ feeder reconnect attempt {AttemptCount} for Queue {Queue} failed. Retrying in {Delay}.",
                            attempt,
                            FeederConfiguration.Queue,
                            delay);

                        attempt++;
                        await Task.Delay(delay, _lifetimeCancellation.Token).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (_lifetimeCancellation.IsCancellationRequested)
            {
                // Application shutdown cancels any pending reconnect delay or connection attempt.
            }
            catch (Exception exception)
            {
                ReportHealth(HealthStatus.Unhealthy, exception);
                Logger.LogError(exception,
                    "RabbitMQ feeder recovery for Queue {Queue} stopped unexpectedly.",
                    FeederConfiguration.Queue);
            }
            finally
            {
                _reconnectLock.Release();
            }
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
            if (Interlocked.Exchange(ref _stopping, 1) != 0)
                return;

            await _lifetimeCancellation.CancelAsync().ConfigureAwait(false);

            try
            {
                await _inFlightMessages.DrainAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await _receiveCancellation.CancelAsync().ConfigureAwait(false);
                await _reconnectLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                try
                {
                    await ReleaseCurrentBrokerResourcesAsync(true, CancellationToken.None).ConfigureAwait(false);
                }
                finally
                {
                    _reconnectLock.Release();
                }
            }

            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task ReleaseCurrentBrokerResourcesAsync(bool close, CancellationToken cancellationToken)
        {
            var connection = _connection;
            var channel = _channel;
            var consumer = _consumer;

            _connection = null;
            _channel = null;
            _consumer = null;

            if (consumer is not null)
                consumer.ReceivedAsync -= HandleReceivedAsync;

            if (connection is not null)
                connection.ConnectionShutdownAsync -= HandleShutdownAsync;

            if (channel is not null)
                channel.ChannelShutdownAsync -= HandleShutdownAsync;

            await DisposeBrokerResourcesAsync(connection, channel, close, cancellationToken).ConfigureAwait(false);
        }

        private async Task DisposeBrokerResourcesAsync(
            IConnection? connection,
            IChannel? channel,
            bool close,
            CancellationToken cancellationToken)
        {
            if (channel is not null)
            {
                try
                {
                    if (close && channel.IsOpen)
                        await channel.CloseAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Logger.LogDebug(exception, "Failed to close RabbitMQ channel for Queue {Queue}.", FeederConfiguration.Queue);
                }

                try
                {
                    await channel.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Logger.LogDebug(exception, "Failed to dispose RabbitMQ channel for Queue {Queue}.", FeederConfiguration.Queue);
                }
            }

            if (connection is not null)
            {
                try
                {
                    if (close && connection.IsOpen)
                        await connection.CloseAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Logger.LogDebug(exception, "Failed to close RabbitMQ connection for Queue {Queue}.", FeederConfiguration.Queue);
                }

                try
                {
                    await connection.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Logger.LogDebug(exception, "Failed to dispose RabbitMQ connection for Queue {Queue}.", FeederConfiguration.Queue);
                }
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await StopAsync().ConfigureAwait(false);

            _receiveCancellation.Dispose();
            _lifetimeCancellation.Dispose();
            _reconnectLock.Dispose();
        }
    }
}
