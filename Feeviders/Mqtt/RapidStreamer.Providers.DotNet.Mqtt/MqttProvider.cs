#if DEBUG
using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
#endif
using Microsoft.Extensions.Logging;
using MQTTnet;
using RapidStreamer.BuildingBlocks.Application.Serializations;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.Mqtt
{
    internal
#if !DEBUG
        sealed
#endif
        class MqttProvider<TMqttProviderMessage, TMqttProviderConfiguration> : AbstractProvider<TMqttProviderMessage, TMqttProviderConfiguration>
        where TMqttProviderMessage : MqttProviderMessage
        where TMqttProviderConfiguration : MqttProviderConfiguration
    {
        private readonly TMqttProviderConfiguration _mqttProviderConfiguration;
        private readonly IMqttClient _mqttClient;

        public MqttProvider(TMqttProviderConfiguration mqttProviderConfiguration, IServiceProvider serviceProvider) : base(serviceProvider)
        {
            _mqttProviderConfiguration = mqttProviderConfiguration;

            var mqttFactory = new MqttClientFactory();
            _mqttClient = mqttFactory.CreateMqttClient();
        }

        protected override async Task InternalExecuteAsync(TMqttProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            try
            {
                if (!_mqttClient.IsConnected)
                    await _mqttClient.ConnectAsync(_mqttProviderConfiguration.ToMqttClientOptions(), cancellationToken);

                var applicationMessageBuilder = new MqttApplicationMessageBuilder()
                    .WithTopic(_mqttProviderConfiguration.Topic)
                    .WithPayload(_mqttProviderConfiguration.SerializerType switch
                    {
                        SerializerType.Json => feederMessage.ToJson(),
                        SerializerType.NJson => feederMessage.ToNJson(),
                        SerializerType.NetJson => feederMessage.ToNetJson(),
                        _ => throw new ArgumentOutOfRangeException()
                    });

#if DEBUG
                if (Activity.Current?.Context is not null)
                    applicationMessageBuilder.WithUserProperty(nameof(ActivityContext), Activity.Current.Context.ToNJsonBase64());

                applicationMessageBuilder.WithUserProperty(nameof(Baggage), Baggage.Current.ToNJsonBase64());
#endif

                var applicationMessage = applicationMessageBuilder.Build();

                await _mqttClient.PublishAsync(applicationMessage, cancellationToken);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception, "error has occured while publishing message to topic {Topic}.", _mqttProviderConfiguration.Topic);
                throw;
            }
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) => Task.CompletedTask;

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _mqttClient.DisconnectAsync();
            _mqttClient.Dispose();
        }
    }
}