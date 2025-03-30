using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Net;
using OpenTelemetry;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.Feeviders.NATS.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.NATS
{
    internal
#if !DEBUG
        sealed
#endif
        class NatsProvider<TNatsProviderMessage, TNatsProviderConfiguration> : AbstractProvider<TNatsProviderMessage, TNatsProviderConfiguration>
        where TNatsProviderMessage : NatsProviderMessage
        where TNatsProviderConfiguration : NatsProviderConfiguration
    {
        private readonly TNatsProviderConfiguration _natsProviderConfiguration;
        private readonly INatsClient _client;
        private readonly INatsJSContext? _jetStreamContext;

        public NatsProvider(TNatsProviderConfiguration natsProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _natsProviderConfiguration = natsProviderConfiguration;
            _client = NatsClientFactory.CreateClient(_natsProviderConfiguration, serviceProvider.GetRequiredService<ILoggerFactory>());

            if (_natsProviderConfiguration.MessagingType == MessagingType.JetStream)
            {
                ArgumentNullException.ThrowIfNull(_natsProviderConfiguration.StreamConfig);

                _jetStreamContext = _client.CreateJetStreamContext();
                _ = _jetStreamContext.CreateStreamAsync(_natsProviderConfiguration.StreamConfig,
                        serviceProvider.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping)
                    .GetAwaiter()
                    .GetResult();
            }
        }

        protected override async Task InternalExecuteAsync(TNatsProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            try
            {
                var natsHeaders = new NatsHeaders();

                if (Activity.Current?.Context is not null)
                    natsHeaders.Add(nameof(ActivityContext), Activity.Current.Context.ToNJsonBase64());

                natsHeaders.Add(nameof(Baggage), Baggage.Current.ToNJsonBase64());

                switch (_natsProviderConfiguration.MessagingType)
                {
                    case MessagingType.Basic:
                        await _client.PublishAsync(subject: _natsProviderConfiguration.Subject,
                            replyTo: _natsProviderConfiguration.ReplyTo,
                            data: feederMessage,
                            cancellationToken: cancellationToken);
                        break;
                    case MessagingType.JetStream:

                        ArgumentNullException.ThrowIfNull(_jetStreamContext);

                        var ack = await _jetStreamContext.PublishAsync(subject: _natsProviderConfiguration.Subject,
                            data: feederMessage,
                            opts: _natsProviderConfiguration.NatsJSPubOpts,
                            cancellationToken: cancellationToken);

                        ack.EnsureSuccess();

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (Exception exception)
            {
                Logger.LogError(exception,
                    "error has occured while producing message to Subject {Subject}.",
                    _natsProviderConfiguration.Subject);
                throw;
            }
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) => Task.CompletedTask;

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _client.DisposeAsync();
        }
    }
}