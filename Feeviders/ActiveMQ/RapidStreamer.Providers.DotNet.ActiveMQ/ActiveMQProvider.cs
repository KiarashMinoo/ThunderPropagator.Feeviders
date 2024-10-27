using Apache.NMS;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Feeviders.ActiveMQ.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Diagnostics;

namespace RapidStreamer.Providers.DotNet.ActiveMQ
{
    internal
#if !DEBUG
        sealed
#endif
        class ActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration> : AbstractProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>
        where TActiveMQProviderMessage : ActiveMQProviderMessage
        where TActiveMQProviderConfiguration : ActiveMQProviderConfiguration
    {
        private readonly TActiveMQProviderConfiguration _activeMQProviderConfiguration;
        private readonly IConnection _connection;
        private readonly IMessageProducer _producer;
        private readonly ISession _session;

        public ActiveMQProvider(TActiveMQProviderConfiguration activeMQProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _activeMQProviderConfiguration = activeMQProviderConfiguration;

            // Create a Connection
            _connection = ActiveMQConnectionFactory.CreateConnection(_activeMQProviderConfiguration);
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
            try
            {
                var message = await _session.CreateBytesMessageAsync(bytes);

#if DEBUG
                if (Activity.Current?.Context is not null)
                    message.Properties.SetBytes(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

                message.Properties.SetBytes(nameof(Baggage), Baggage.Current.ToNJsonBytes());
#endif

                await _producer.SendAsync(message);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while producing message to queue {Queue}.",
                    _activeMQProviderConfiguration.Queue);
                throw;
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _producer.CloseAsync();
            await _session.CloseAsync();
            await _connection.CloseAsync();

            _producer.Dispose();
            _session.Dispose();
            _connection.Dispose();
        }
    }
}