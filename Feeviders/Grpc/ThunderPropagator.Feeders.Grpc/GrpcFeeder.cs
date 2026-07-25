using System.Diagnostics;
using System.Runtime.CompilerServices;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Features;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.Grpc.SharedKernel;
using ThunderPropagator.Feeviders.Grpc.SharedKernel.Protos;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeders.Grpc
{
    internal
#if !DEBUG
        sealed
#endif
        partial class GrpcFeeder<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration> : IterativeFeeder<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TGrpcFeederMessage : GrpcFeederMessage
        where TGrpcFeederConfiguration : GrpcFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4500, Level = LogLevel.Warning, Message = "{FeederName}/{ChannelName} is disabled (IsEnabled=false), skipping broker connection.")]
            public static partial void FeederDisabled(ILogger logger, string feederName, string channelName);

            [LoggerMessage(EventId = 4502, Level = LogLevel.Information, Message = "gRPC feeder on Topic {Topic} reconnected after {AttemptCount} attempt(s).")]
            public static partial void ReconnectSucceeded(ILogger logger, string topic, int attemptCount);

            [LoggerMessage(EventId = 4503, Level = LogLevel.Warning, Message = "gRPC feeder reconnect attempt {AttemptCount} for Topic {Topic} failed. Retrying in {Delay}.")]
            public static partial void ReconnectAttemptFailed(ILogger logger, Exception exception, int attemptCount, string topic, TimeSpan delay);

            [LoggerMessage(EventId = 4504, Level = LogLevel.Error, Message = "gRPC feeder on Topic {Topic} exhausted its reconnect attempts.")]
            public static partial void ReconnectAttemptsExhausted(ILogger logger, string topic);
        }

        private readonly GrpcChannel _channel;
        private readonly GrpcFeeviderService.GrpcFeeviderServiceClient _client;
        private readonly FormatDeserializerInvoker _formatDeserializerInvoker;

        public GrpcFeeder(TChannel channel,
            TGrpcFeederConfiguration feederConfiguration,
            IFeederHandler<TChannel, TGrpcFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, feederConfiguration, feederHandler, serviceProvider)
        {
            _channel = GrpcChannelFactory.CreateChannel(feederConfiguration);
            _client = new GrpcFeeviderService.GrpcFeeviderServiceClient(_channel);
            _formatDeserializerInvoker = serviceProvider.GetRequiredService<FormatDeserializerInvoker>();
        }

        protected override async Task StartingAsync(CancellationToken cancellationToken = default)
        {
            if (!FeederConfiguration.IsEnabled)
            {
                Log.FeederDisabled(Logger, GetType().Name, Channel.Metadata.ChannelName);
                return;
            }

            await base.StartingAsync(cancellationToken).ConfigureAwait(false);
        }

        protected override async IAsyncEnumerable<FeederReceivedMessage<TGrpcFeederMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = new())
        {
            if (!FeederConfiguration.IsEnabled)
            {
                await Task.Yield();
                yield break;
            }

            var attempt = 1;

            while (!cancellationToken.IsCancellationRequested)
            {
                using var call = _client.Subscribe(new GrpcSubscribeRequest { Topic = FeederConfiguration.Topic }, cancellationToken: cancellationToken);

                var streamFailed = false;
                Exception? streamException = null;

                while (true)
                {
                    GrpcEnvelope envelope;

                    try
                    {
                        if (!await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
                            break;

                        envelope = call.ResponseStream.Current;
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        streamFailed = true;
                        streamException = exception;
                        break;
                    }

                    if (attempt > 1)
                        Log.ReconnectSucceeded(Logger, FeederConfiguration.Topic, attempt - 1);
                    attempt = 1;

                    var stopwatch = Stopwatch.StartNew();
                    var (message, activityContext, baggage) = GrpcEnvelopeTranslator.ToFeederMessage<TGrpcFeederMessage>(
                        envelope, _formatDeserializerInvoker, FeederConfiguration.SerializerType);

                    using var activity = activityContext.HasValue
                        ? GrpcFeederExtensions.ActivitySource.StartActivity("grpc receive", ActivityKind.Consumer, activityContext.Value)
                        : GrpcFeederExtensions.ActivitySource.StartActivity("grpc receive", ActivityKind.Consumer);
                    activity?.SetTag("messaging.system", "grpc");
                    activity?.SetTag("messaging.destination.name", FeederConfiguration.Topic);
                    activity?.SetTag("messaging.operation", "receive");

                    var settled = false;
                    try
                    {
                        yield return new FeederReceivedMessage<TGrpcFeederMessage>(message, activityContext, baggage);
                        settled = true;
                    }
                    finally
                    {
                        stopwatch.Stop();
                        if (settled)
                        {
                            GrpcFeederExtensions.MessagesReceived.Add(1);
                        }
                        else
                        {
                            GrpcFeederExtensions.MessagesReceiveFailed.Add(1);
                            activity?.SetStatus(ActivityStatusCode.Error);
                        }
                        GrpcFeederExtensions.ReceiveDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
                    }
                }

                if (cancellationToken.IsCancellationRequested || !streamFailed)
                {
                    if (cancellationToken.IsCancellationRequested)
                        yield break;

                    continue;
                }

                if (FeederConfiguration.MaxReconnectAttempts >= 0 && attempt > FeederConfiguration.MaxReconnectAttempts)
                {
                    Log.ReconnectAttemptsExhausted(Logger, FeederConfiguration.Topic);
                    ReportHealth(HealthStatus.Unhealthy, streamException);
                    yield break;
                }

                var delay = GrpcReconnectDelay.Calculate(
                    FeederConfiguration.ReconnectInitialDelay,
                    FeederConfiguration.ReconnectMaxDelay,
                    attempt);

                Log.ReconnectAttemptFailed(Logger, streamException!, attempt, FeederConfiguration.Topic, delay);
                ReportHealth(HealthStatus.Unhealthy, streamException);

                try
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }

                attempt++;
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            await _channel.ShutdownAsync().ConfigureAwait(false);
            _channel.Dispose();
        }
    }
}
