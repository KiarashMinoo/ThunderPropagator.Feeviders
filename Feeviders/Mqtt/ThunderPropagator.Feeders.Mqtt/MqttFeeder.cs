using System.Buffers;
using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Packets;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ThunderPropagator.Feeders.Mqtt
{
    internal
#if !DEBUG
        sealed
#endif
        class MqttFeeder<TChannel, TMqttFeederMessage, TMqttFeederConfiguration> : DelegativeFeeder<TChannel, TMqttFeederMessage, TMqttFeederConfiguration>
        where TChannel : class, IChannel
        where TMqttFeederMessage : MqttFeederMessage
        where TMqttFeederConfiguration : MqttFeederConfiguration
    {
        private readonly TMqttFeederConfiguration _mqttFeederConfiguration;
        private IMqttClient? _mqttClient;

        public MqttFeeder(TChannel channel,
            TMqttFeederConfiguration mqttFeederConfiguration,
            IFeederHandler<TChannel, TMqttFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, mqttFeederConfiguration, feederHandler, serviceProvider)
        {
            _mqttFeederConfiguration = mqttFeederConfiguration;
            var applicationLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

            _ = Task.Run(() => StartAsync_CatchAll(applicationLifetime.ApplicationStopping), applicationLifetime.ApplicationStopping);

            HealthName = $"feeder_{nameof(Mqtt)}_{_mqttFeederConfiguration.Topic}";
            HealthTags = [.. HealthTags, nameof(Mqtt), _mqttFeederConfiguration.Topic];

            Logger.LogInformation(
                "{FeederName}/{ChannelName} on topic {Topic} has subscribed.",
                GetType().Name,
                channel.Metadata.ChannelName,
                _mqttFeederConfiguration.Topic);
        }

        private async Task StartAsync(object? state)
        {
            if (state is not CancellationToken cancellationToken)
                cancellationToken = CancellationToken.None;

            var mqttFactory = new MqttClientFactory();
            _mqttClient = mqttFactory.CreateMqttClient();

            await _mqttClient.ConnectAsync(_mqttFeederConfiguration.ToMqttClientOptions(), cancellationToken).ConfigureAwait(false);

            _mqttClient.ApplicationMessageReceivedAsync += async args =>
            {
                try
                {
                    var activityContext = args.ApplicationMessage.UserProperties.Find(x => x.Name == nameof(ActivityContext))?.ReadValueAsString().FromNJsonBase64<ActivityContext>();
                    var baggage = args.ApplicationMessage.UserProperties.Find(x => x.Name == nameof(Baggage))?.ReadValueAsString().FromNJsonBase64<Baggage>();

                    await ReceiveAsync(args.ApplicationMessage.Payload.ToArray(),
                        activityContext,
                        baggage,
                        new Dictionary<string, object?>
                        {
                            { nameof(args.ClientId), args.ClientId },
                            { nameof(args.Tag), args.Tag },
                            { nameof(args.ApplicationMessage.Topic), args.ApplicationMessage.Topic },
                        },
                        cancellationToken).ConfigureAwait(false);

                    ReportHealth(HealthStatus.Healthy);
                }
                catch (Exception exception)
                {
                    ReportHealth(HealthStatus.Unhealthy, exception);

                    Logger.LogError(exception, "error has occured while consuming messages on Topic {Topic}.", FeederConfiguration.Topic);
                }
            };

            var mqttSubscribeOptionsBuilder = mqttFactory.CreateSubscribeOptionsBuilder();

            if (_mqttFeederConfiguration.SubscriptionIdentifier is not null)
                mqttSubscribeOptionsBuilder.WithSubscriptionIdentifier(_mqttFeederConfiguration.SubscriptionIdentifier.Value);

            var mqttSubscribeOptions = mqttSubscribeOptionsBuilder.Build();

            await _mqttClient.SubscribeAsync(mqttSubscribeOptions, cancellationToken).ConfigureAwait(false);
        }

        // StartAsync runs as a background task - ensure top-level exceptions are observed and logged
        private async Task StartAsync_CatchAll(object? state)
        {
            try
            {
                await StartAsync(state).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Unhandled exception in MQTT feeder background loop.");
                ReportHealth(HealthStatus.Unhealthy, ex);
            }
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                if (_mqttClient is not null)
                    await _mqttClient.DisconnectAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disconnecting MQTT client.");
            }

            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        protected override void DisposeManagedResources()
        {
            try
            {
                _mqttClient?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing MQTT client.");
            }
            
            base.DisposeManagedResources();
        }
    }
}
