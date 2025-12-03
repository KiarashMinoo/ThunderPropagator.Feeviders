using Microsoft.Extensions.Logging;
using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Providers.DotNet.SharedKernel;
using System.Diagnostics;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using RapidStreamer.Feeviders.Pulsar.SharedKernel;

namespace RapidStreamer.Providers.DotNet.Pulsar
{
    internal
#if !DEBUG
        sealed
#endif
        class PulsarProvider<TPulsarProviderMessage, TPulsarProviderConfiguration> : AbstractProvider<TPulsarProviderMessage, TPulsarProviderConfiguration>
        where TPulsarProviderMessage : PulsarProviderMessage
        where TPulsarProviderConfiguration : PulsarProviderConfiguration
    {
        private readonly TPulsarProviderConfiguration _pulsarProviderConfiguration;
        private readonly IPulsarClient _client;
        private readonly IProducer<TPulsarProviderMessage> _producer;

        public PulsarProvider(TPulsarProviderConfiguration pulsarProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _pulsarProviderConfiguration = pulsarProviderConfiguration;
            _client = PulsarClientFactory.CreateClient(pulsarProviderConfiguration);
            var schema = new JsonSchema<TPulsarProviderMessage>(pulsarProviderConfiguration.SerializerType);

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
            try
            {
                if (Activity.Current?.Context is not null)
                    feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

                feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());

                await _producer.Send(feederMessage, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while producing message to queue {Topic}.",
                    _pulsarProviderConfiguration.Topic);
                throw;
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