using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using MQTTnet;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Mqtt
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
                    await _mqttClient.ConnectAsync(_mqttProviderConfiguration.ToMqttClientOptions(), cancellationToken).ConfigureAwait(false);

                var applicationMessageBuilder = new MqttApplicationMessageBuilder()
                    .WithTopic(_mqttProviderConfiguration.Topic)
                    .WithPayload(_mqttProviderConfiguration.SerializerType switch
                    {
                        SerializerType.Json => feederMessage.ToJson(),
                        SerializerType.NJson => feederMessage.ToNJson(),
                        SerializerType.NetJson => feederMessage.ToNetJson(),
                        _ => throw new ArgumentOutOfRangeException()
                    });

                if (Activity.Current?.Context is not null)
                    applicationMessageBuilder.WithUserProperty(nameof(ActivityContext), Activity.Current.Context.ToNJsonBase64());

                applicationMessageBuilder.WithUserProperty(nameof(Baggage), Baggage.Current.ToNJsonBase64());

                var applicationMessage = applicationMessageBuilder.Build();

                await _mqttClient.PublishAsync(applicationMessage, cancellationToken).ConfigureAwait(false);
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
            try
            {
                await _mqttClient.DisconnectAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disconnecting MQTT client.");
            }

            try
            {
                _mqttClient?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Exception while disposing MQTT client.");
            }
        }
    }
}