using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;
using ThunderPropagator.Feeviders.NATS.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.NATS
{
    internal
#if !DEBUG
        sealed
#endif
        partial class NatsProvider<TNatsProviderMessage, TNatsProviderConfiguration> : AbstractProvider<TNatsProviderMessage, TNatsProviderConfiguration>
        where TNatsProviderMessage : NatsProviderMessage
        where TNatsProviderConfiguration : NatsProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4304, Level = LogLevel.Error, Message = "error has occured while producing message to Subject {Subject}.")]
            public static partial void ProduceException(ILogger logger, Exception exception, string subject);

            [LoggerMessage(EventId = 4305, Level = LogLevel.Error, Message = "Failed to initialize JetStream context.")]
            public static partial void JetStreamContextInitializationFailed(ILogger logger, Exception exception);
        }

        private readonly TNatsProviderConfiguration _natsProviderConfiguration;
        private readonly INatsClient _client;
        private readonly INatsJSContext? _jetStreamContext;
        // Background initialization task for JetStream context
        private readonly Task? _jetStreamInitTask;

        public NatsProvider(TNatsProviderConfiguration natsProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _natsProviderConfiguration = natsProviderConfiguration;
            _client = NatsClientFactory.CreateClient(_natsProviderConfiguration, serviceProvider.GetRequiredService<ILoggerFactory>());

            if (_natsProviderConfiguration.MessagingType == MessagingType.JetStream)
            {
                ArgumentNullException.ThrowIfNull(_natsProviderConfiguration.StreamConfig);

                _jetStreamContext = _client.CreateJetStreamContext();
                _jetStreamInitTask = InitializeJetStreamContextAsync(_natsProviderConfiguration.StreamConfig, serviceProvider.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);
            }
        }

        protected override async Task InternalExecuteAsync(TNatsProviderMessage feederMessage, CancellationToken cancellationToken = default)
        {
            using var activity = NatsProviderExtensions.ActivitySource.StartActivity("nats publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "nats");
            activity?.SetTag("messaging.destination.name", _natsProviderConfiguration.Subject);
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();

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
                            cancellationToken: cancellationToken).ConfigureAwait(false);
                        break;
                    case MessagingType.JetStream:

                        // ensure the jetstream context completed initialization before use
                        if (_jetStreamInitTask is not null)
                            await _jetStreamInitTask.ConfigureAwait(false);

                        ArgumentNullException.ThrowIfNull(_jetStreamContext);

                        var ack = await _jetStreamContext.PublishAsync(subject: _natsProviderConfiguration.Subject,
                            data: feederMessage,
                            opts: _natsProviderConfiguration.NatsJSPubOpts,
                            cancellationToken: cancellationToken).ConfigureAwait(false);

                        ack.EnsureSuccess();

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                NatsProviderExtensions.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                NatsProviderExtensions.MessagesPublishFailed.Add(1);

                Log.ProduceException(Logger, exception, _natsProviderConfiguration.Subject);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                NatsProviderExtensions.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) => Task.CompletedTask;

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _client.DisposeAsync();
        }

        private async Task InitializeJetStreamContextAsync(StreamConfig streamConfig, CancellationToken cancellationToken)
        {
            try
            {
                await _jetStreamContext!.CreateStreamAsync(streamConfig, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.JetStreamContextInitializationFailed(Logger, ex);
                throw;
            }
        }
    }
}