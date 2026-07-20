using Apache.NMS;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Feeviders.ActiveMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using System.Diagnostics;

namespace ThunderPropagator.Providers.DotNet.ActiveMQ
{
    internal
#if !DEBUG
        sealed
#endif
        partial class ActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration> : AbstractProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>
        where TActiveMQProviderMessage : ActiveMQProviderMessage
        where TActiveMQProviderConfiguration : ActiveMQProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4506, Level = LogLevel.Error, Message = "error has occured while producing message to queue {Queue}.")]
            public static partial void ProduceError(ILogger logger, Exception exception, string queue);

            [LoggerMessage(EventId = 4507, Level = LogLevel.Warning, Message = "Exception while closing ActiveMQ producer.")]
            public static partial void ProducerCloseError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4508, Level = LogLevel.Warning, Message = "Exception while closing ActiveMQ session.")]
            public static partial void SessionCloseError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4509, Level = LogLevel.Warning, Message = "Exception while closing ActiveMQ connection.")]
            public static partial void ConnectionCloseError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4510, Level = LogLevel.Warning, Message = "Exception while disposing ActiveMQ producer.")]
            public static partial void ProducerDisposeError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4511, Level = LogLevel.Warning, Message = "Exception while disposing ActiveMQ session.")]
            public static partial void SessionDisposeError(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4512, Level = LogLevel.Warning, Message = "Exception while disposing ActiveMQ connection.")]
            public static partial void ConnectionDisposeError(ILogger logger, Exception exception);
        }

        private readonly TActiveMQProviderConfiguration _activeMQProviderConfiguration;
        private readonly IConnection _connection;
        private readonly IMessageProducer _producer;
        private readonly ISession _session;

        public ActiveMQProvider(TActiveMQProviderConfiguration activeMQProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _activeMQProviderConfiguration = activeMQProviderConfiguration;

            // Create a Connection
            _connection = ActiveMQFeeviderConnectionFactory.CreateConnection(_activeMQProviderConfiguration);
            _connection.Start();

            // Create a Session
            _session = _connection.CreateSession();

            // Get the destination (Topic or Queue)
            IDestination destination = _session.GetQueue(_activeMQProviderConfiguration.Queue);

            // Create a MessageProducer from the Session to the Topic or Queue
            _producer = _session.CreateProducer(destination);

            if (_activeMQProviderConfiguration.DeliveryMode != null)
                _producer.DeliveryMode = _activeMQProviderConfiguration.DeliveryMode.Value;

            if (_activeMQProviderConfiguration.TimeToLive != null)
                _producer.TimeToLive = _activeMQProviderConfiguration.TimeToLive.Value;

            if (_activeMQProviderConfiguration.ProducerRequestTimeout != null)
                _producer.RequestTimeout = _activeMQProviderConfiguration.ProducerRequestTimeout.Value;

            if (_activeMQProviderConfiguration.Priority != null)
                _producer.Priority = _activeMQProviderConfiguration.Priority.Value;

            if (_activeMQProviderConfiguration.DisableMessageID != null)
                _producer.DisableMessageID = _activeMQProviderConfiguration.DisableMessageID.Value;

            if (_activeMQProviderConfiguration.DisableMessageTimestamp != null)
                _producer.DisableMessageTimestamp = _activeMQProviderConfiguration.DisableMessageTimestamp.Value;

            if (_activeMQProviderConfiguration.DeliveryDelay != null)
                _producer.DeliveryDelay = _activeMQProviderConfiguration.DeliveryDelay.Value;
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            using var activity = ActiveMQTelemetry.ActivitySource.StartActivity("activemq publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "activemq");
            activity?.SetTag("messaging.destination.name", _activeMQProviderConfiguration.Queue);
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var message = await _session.CreateBytesMessageAsync(bytes).ConfigureAwait(false);

                if (Activity.Current?.Context is not null)
                    message.Properties.SetBytes(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

                message.Properties.SetBytes(nameof(Baggage), Baggage.Current.ToNJsonBytes());

                await _producer.SendAsync(message).ConfigureAwait(false);

                ActiveMQTelemetry.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                ActiveMQTelemetry.MessagesPublishFailed.Add(1);
                Log.ProduceError(Logger, exception, _activeMQProviderConfiguration.Queue);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                ActiveMQTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            try
            {
                await _producer.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.ProducerCloseError(Logger, ex);
            }

            try
            {
                await _session.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.SessionCloseError(Logger, ex);
            }

            try
            {
                await _connection.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.ConnectionCloseError(Logger, ex);
            }

            try
            {
                _producer?.Dispose();
            }
            catch (Exception ex)
            {
                Log.ProducerDisposeError(Logger, ex);
            }

            try
            {
                _session?.Dispose();
            }
            catch (Exception ex)
            {
                Log.SessionDisposeError(Logger, ex);
            }

            try
            {
                _connection?.Dispose();
            }
            catch (Exception ex)
            {
                Log.ConnectionDisposeError(Logger, ex);
            }
        }
    }
}
