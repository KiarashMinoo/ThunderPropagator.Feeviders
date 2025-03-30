using System.Buffers;
using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace RapidStreamer.Feeders.Mqtt
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

            new Thread(Start).Start(applicationLifetime.ApplicationStopping);

            HealthName = $"feeder_{nameof(Mqtt)}_{_mqttFeederConfiguration.Topic}";
            HealthTags = [.. HealthTags, nameof(Mqtt), _mqttFeederConfiguration.Topic];

            Logger.LogInformation($"{GetType().GetTypeInfo().Name}/{channel.Metadata.ChannelName} on topic {{Topic}} has subscribed.",
                string.Join(", ", _mqttFeederConfiguration.Topic));
        }

        private async void Start(object? state)
        {
            if (state is not CancellationToken cancellationToken)
                cancellationToken = CancellationToken.None;

            var mqttFactory = new MqttClientFactory();
            _mqttClient = mqttFactory.CreateMqttClient();

            await _mqttClient.ConnectAsync(_mqttFeederConfiguration.ToMqttClientOptions(), cancellationToken);

            _mqttClient.ApplicationMessageReceivedAsync += async args =>
            {
                try
                {
                    var activityContext = args.ApplicationMessage.UserProperties.Find(x => x.Name == nameof(ActivityContext))?.Value.FromNJsonBase64<ActivityContext>();
                    var baggage = args.ApplicationMessage.UserProperties.Find(x => x.Name == nameof(Baggage))?.Value.FromNJsonBase64<Baggage>();

                    await ReceiveAsync(args.ApplicationMessage.Payload.ToArray(),
                        activityContext,
                        baggage,
                        new Dictionary<string, object?>
                        {
                            { nameof(args.ClientId), args.ClientId },
                            { nameof(args.Tag), args.Tag },
                            { nameof(args.ApplicationMessage.Topic), args.ApplicationMessage.Topic },
                        },
                        cancellationToken);

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

            await _mqttClient.SubscribeAsync(mqttSubscribeOptions, cancellationToken);
        }

        protected override async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (_mqttClient is not null)
                await _mqttClient.DisconnectAsync(cancellationToken: cancellationToken);

            await base.StopAsync(cancellationToken);
        }

        protected override void DisposeManagedResources()
        {
            _mqttClient?.Dispose();
            base.DisposeManagedResources();
        }
    }
}