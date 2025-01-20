#if DEBUG
using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
#endif
using Apache.NMS;
using Microsoft.Extensions.Logging;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.ActiveMQ.SharedKernel;
using System.Reflection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RapidStreamer.Application;

namespace RapidStreamer.Feeders.ActiveMQ
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
                    Baggage? baggage = null;

#if DEBUG
                    if (message.Properties.Contains(nameof(ActivityContext)))
                        activityContext = message.Properties.GetBytes(nameof(ActivityContext)).FromNJsonBytes<ActivityContext>();

                    if (message.Properties.Contains(nameof(Baggage)))
                        baggage = message.Properties.GetBytes(nameof(Baggage)).FromNJsonBytes<Baggage>();
#endif

                    switch (message)
                    {
                        case IObjectMessage { Body: TActiveMQFeederMessage activeMQFeederMessage }:
                            await ReceiveAsync(activeMQFeederMessage, activityContext, baggage);
                            break;
                        case ITextMessage textMessage:
                            await ReceiveAsync(textMessage.Text, activityContext, baggage);
                            break;
                        case IBytesMessage bytesMessage:
                            await ReceiveAsync(bytesMessage.Content, activityContext, baggage);
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
            await _consumer.CloseAsync();
            await _session.CloseAsync();
            await _connection.CloseAsync();
        }

        protected override void DisposeManagedResources()
        {
            _consumer.Dispose();
            _session.Dispose();
            _connection.Dispose();
        }
    }
}