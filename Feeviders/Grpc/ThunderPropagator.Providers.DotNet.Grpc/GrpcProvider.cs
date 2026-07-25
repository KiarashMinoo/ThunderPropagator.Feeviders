using System.Diagnostics;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.Feeviders.Grpc.SharedKernel;
using ThunderPropagator.Feeviders.Grpc.SharedKernel.Protos;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Grpc
{
    internal
#if !DEBUG
        sealed
#endif
        partial class GrpcProvider<TGrpcProviderMessage, TGrpcProviderConfiguration> : AbstractProvider<TGrpcProviderMessage, TGrpcProviderConfiguration>
        where TGrpcProviderMessage : GrpcProviderMessage
        where TGrpcProviderConfiguration : GrpcProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4505, Level = LogLevel.Error, Message = "error has occured while producing message to Topic {Topic}.")]
            public static partial void ProduceException(ILogger logger, Exception exception, string topic);
        }

        private readonly TGrpcProviderConfiguration _grpcProviderConfiguration;
        private readonly GrpcChannel? _channel;
        private readonly GrpcFeeviderService.GrpcFeeviderServiceClient _client;

        public GrpcProvider(TGrpcProviderConfiguration grpcProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _grpcProviderConfiguration = grpcProviderConfiguration;
            _channel = GrpcChannelFactory.CreateChannel(grpcProviderConfiguration);
            _client = new GrpcFeeviderService.GrpcFeeviderServiceClient(_channel);
        }

        internal GrpcProvider(TGrpcProviderConfiguration grpcProviderConfiguration,
            IServiceProvider serviceProvider,
            GrpcFeeviderService.GrpcFeeviderServiceClient client)
            : base(serviceProvider)
        {
            _grpcProviderConfiguration = grpcProviderConfiguration;
            _client = client;
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            using var activity = GrpcProviderExtensions.ActivitySource.StartActivity("grpc publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "grpc");
            activity?.SetTag("messaging.destination.name", _grpcProviderConfiguration.Topic);
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();

            try
            {
                var envelope = GrpcEnvelopeTranslator.ToEnvelope(_grpcProviderConfiguration.Topic, bytes);

                await _client.PublishAsync(envelope, cancellationToken: cancellationToken).ConfigureAwait(false);

                GrpcProviderExtensions.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                GrpcProviderExtensions.MessagesPublishFailed.Add(1);

                Log.ProduceException(Logger, exception, _grpcProviderConfiguration.Topic);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                GrpcProviderExtensions.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            if (_channel is not null)
            {
                await _channel.ShutdownAsync().ConfigureAwait(false);
                _channel.Dispose();
            }
        }
    }
}
