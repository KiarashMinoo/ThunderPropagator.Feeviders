using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Feeviders.Mqtt.SharedKernel;
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
        partial class MqttProvider<TMqttProviderMessage, TMqttProviderConfiguration> : AbstractProvider<TMqttProviderMessage, TMqttProviderConfiguration>
        where TMqttProviderMessage : MqttProviderMessage
        where TMqttProviderConfiguration : MqttProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4205, Level = LogLevel.Error, Message = "error has occured while publishing message to topic {Topic}.")]
            public static partial void PublishException(ILogger logger, Exception exception, string topic);

            [LoggerMessage(EventId = 4206, Level = LogLevel.Warning, Message = "Exception while disconnecting MQTT client.")]
            public static partial void DisconnectException(ILogger logger, Exception exception);

            [LoggerMessage(EventId = 4207, Level = LogLevel.Warning, Message = "Exception while disposing MQTT client.")]
            public static partial void DisposeException(ILogger logger, Exception exception);
        }

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
            using var activity = MqttTelemetry.ActivitySource.StartActivity("mqtt publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "mqtt");
            activity?.SetTag("messaging.destination.name", _mqttProviderConfiguration.Topic);
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();

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

                MqttTelemetry.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                Log.PublishException(Logger, exception, _mqttProviderConfiguration.Topic);

                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                MqttTelemetry.MessagesPublishFailed.Add(1);

                throw;
            }
            finally
            {
                stopwatch.Stop();
                MqttTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
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
                Log.DisconnectException(Logger, ex);
            }

            try
            {
                _mqttClient?.Dispose();
            }
            catch (Exception ex)
            {
                Log.DisposeException(Logger, ex);
            }
        }
    }
}
