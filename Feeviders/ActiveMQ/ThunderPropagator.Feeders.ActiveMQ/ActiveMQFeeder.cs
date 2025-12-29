using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Apache.NMS;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.ActiveMQ.SharedKernel;
using System.Reflection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ThunderPropagator.Application;

namespace ThunderPropagator.Feeders.ActiveMQ
{
    internal
#if !DEBUG
        sealed
#endif
        class ActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration> : DelegativeFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TActiveMQFeederMessage : ActiveMQFeederMessage
        where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration
    {
        private readonly IConnection _connection;
        private readonly IMessageConsumer _consumer;
        private readonly ISession _session;

        public ActiveMQFeeder(TChannel channel,
            TActiveMQFeederConfiguration activeMQFeederConfiguration,
            IFeederHandler<TChannel, TActiveMQFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, activeMQFeederConfiguration, feederHandler, serviceProvider)
        {
            // Create a Connection
            _connection = ActiveMQFeeviderConnectionFactory.CreateConnection(activeMQFeederConfiguration);
            _connection.Start();

            // Create a Session
            _session = _connection.CreateSession();

            // Get the destination (Topic or Queue)
            IDestination destination = _session.GetQueue(activeMQFeederConfiguration.Queue);

            // Create a MessageProducer from the Session to the Topic or Queue
            _consumer = _session.CreateConsumer(destination);

            _consumer.Listener += async message =>
            {
                try
                {
                    ActivityContext? activityContext = null;
                    if (message.Properties.Contains(nameof(ActivityContext)))
                        activityContext = message.Properties.GetBytes(nameof(ActivityContext)).FromNJsonBytes<ActivityContext>();

                    Baggage? baggage = null;
                    if (message.Properties.Contains(nameof(Baggage)))
                        baggage = message.Properties.GetBytes(nameof(Baggage)).FromNJsonBytes<Baggage>();

                    switch (message)
                    {
                        case IObjectMessage { Body: TActiveMQFeederMessage activeMQFeederMessage }:
                            await ReceiveAsync(activeMQFeederMessage, activityContext, baggage).ConfigureAwait(false);
                            break;
                        case ITextMessage textMessage:
                            await ReceiveAsync(textMessage.Text, activityContext, baggage).ConfigureAwait(false);
                            break;
                        case IBytesMessage bytesMessage:
                            await ReceiveAsync(bytesMessage.Content, activityContext, baggage).ConfigureAwait(false);
                            break;
                    }
                }
                catch (Exception exception)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);
                }
            };

            Logger.LogInformation("{Name}/{ChannelName} on Queue {Queue} has configured.", GetType().GetTypeInfo().Name, channel.Metadata.ChannelName,
                activeMQFeederConfiguration.Queue);

            HealthName = $"feeder_{nameof(ActiveMQ)}_{activeMQFeederConfiguration.Queue}";
            HealthTags = [.. HealthTags, nameof(ActiveMQ), activeMQFeederConfiguration.Queue];
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await _consumer.CloseAsync().ConfigureAwait(false);
                await _session.CloseAsync().ConfigureAwait(false);
                await _connection.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while closing ActiveMQ resources.");
            }
        }

        protected override void DisposeManagedResources()
        {
            try
            {
                _consumer?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing ActiveMQ consumer.");
            }

            try
            {
                _session?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing ActiveMQ session.");
            }

            try
            {
                _connection?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing ActiveMQ connection.");
            }
        }
    }
}