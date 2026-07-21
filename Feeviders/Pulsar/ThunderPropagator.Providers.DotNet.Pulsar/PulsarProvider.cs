using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using System.Diagnostics;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeviders.Pulsar.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Pulsar
{
    internal
#if !DEBUG
        sealed
#endif
        partial class PulsarProvider<TPulsarProviderMessage, TPulsarProviderConfiguration> : AbstractProvider<TPulsarProviderMessage, TPulsarProviderConfiguration>
        where TPulsarProviderMessage : PulsarProviderMessage
        where TPulsarProviderConfiguration : PulsarProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4403, Level = LogLevel.Error, Message = "error has occured while producing message to queue {Topic}.")]
            public static partial void ProduceException(ILogger logger, Exception exception, string topic);
        }

        private readonly TPulsarProviderConfiguration _pulsarProviderConfiguration;
        private readonly IPulsarClient _client;
        private readonly IProducer<TPulsarProviderMessage> _producer;

        public PulsarProvider(TPulsarProviderConfiguration pulsarProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _pulsarProviderConfiguration = pulsarProviderConfiguration;
            _client = PulsarClientFactory.CreateClient(pulsarProviderConfiguration);
            var formatDeserializerInvoker = serviceProvider.GetRequiredService<FormatDeserializerInvoker>();
            var formatSerializerInvoker = serviceProvider.GetRequiredService<FormatSerializerInvoker>();

            var schema = new JsonSchema<TPulsarProviderMessage>(formatDeserializerInvoker, formatSerializerInvoker, pulsarProviderConfiguration.SerializerType);

            var producerOptions = new ProducerOptions<TPulsarProviderMessage>(pulsarProviderConfiguration.Topic, schema);

            if (_pulsarProviderConfiguration.AttachTraceInfoToMessages != null)
                producerOptions.AttachTraceInfoToMessages = _pulsarProviderConfiguration.AttachTraceInfoToMessages.Value;

            if (_pulsarProviderConfiguration.CompressionType != null)
                producerOptions.CompressionType = _pulsarProviderConfiguration.CompressionType.Value;

            if (_pulsarProviderConfiguration.InitialSequenceId != null)
                producerOptions.InitialSequenceId = _pulsarProviderConfiguration.InitialSequenceId.Value;

            if (_pulsarProviderConfiguration.ProducerAccessMode != null)
                producerOptions.ProducerAccessMode = _pulsarProviderConfiguration.ProducerAccessMode.Value;

            if (!string.IsNullOrWhiteSpace(_pulsarProviderConfiguration.ProducerName))
                producerOptions.ProducerName = _pulsarProviderConfiguration.ProducerName;

            if (_pulsarProviderConfiguration.MaxPendingMessages != null)
                producerOptions.MaxPendingMessages = _pulsarProviderConfiguration.MaxPendingMessages.Value;

            if (_pulsarProviderConfiguration.ProducerProperties != null)
                producerOptions.ProducerProperties = _pulsarProviderConfiguration.ProducerProperties;

            _producer = _client.CreateProducer(producerOptions);
        }

        protected override async Task InternalExecuteAsync(TPulsarProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            using var activity = PulsarProviderExtensions.ActivitySource.StartActivity("pulsar publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "pulsar");
            activity?.SetTag("messaging.destination.name", _pulsarProviderConfiguration.Topic);
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();

            try
            {
                if (Activity.Current?.Context is not null)
                    feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

                feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());

                await _producer.Send(feederMessage, cancellationToken).ConfigureAwait(false);

                PulsarProviderExtensions.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                PulsarProviderExtensions.MessagesPublishFailed.Add(1);
                Log.ProduceException(Logger, exception, _pulsarProviderConfiguration.Topic);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                PulsarProviderExtensions.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) => Task.CompletedTask;

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _producer.DisposeAsync();
            await _client.DisposeAsync();
        }
    }
}
