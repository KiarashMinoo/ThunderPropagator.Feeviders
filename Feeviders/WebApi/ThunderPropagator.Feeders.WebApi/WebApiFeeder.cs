using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using ThunderPropagator.Application;

namespace ThunderPropagator.Feeders.WebApi
{
    internal
#if !DEBUG
        sealed
#endif
        partial class WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration> : DelegativeFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TWebApiFeederMessage : WebApiFeederMessage
        where TWebApiFeederConfiguration : WebApiFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4800, Level = LogLevel.Information, Message = "{Name}/{ChannelName} on Endpoint {Endpoint} has configured.")]
            public static partial void FeederConfigured(ILogger logger, string name, string channelName, string endpoint);

            [LoggerMessage(EventId = 4801, Level = LogLevel.Error, Message = "Error while processing a WebApi message on Endpoint {Endpoint}.")]
            public static partial void ProcessError(ILogger logger, Exception exception, string endpoint);
        }

        public WebApiFeeder(TChannel channel,
            TWebApiFeederConfiguration webApiFeederConfiguration,
            IFeederHandler<TChannel, TWebApiFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, webApiFeederConfiguration, feederHandler, serviceProvider)
        {
            Log.FeederConfigured(Logger, GetType().GetTypeInfo().Name, channel.Metadata.ChannelName, webApiFeederConfiguration.Path);

            HealthName = $"feeder_{nameof(WebApi)}_{webApiFeederConfiguration.Path.Replace("/", "_")}";
            HealthTags = [.. HealthTags, nameof(WebApi), webApiFeederConfiguration.Path.Replace("/", "_")];
        }

        internal async ValueTask EnqueueAsync(string rawMessage, string? traceparent, string? tracestate, CancellationToken cancellationToken = default)
        {
            using var activity = traceparent is not null && ActivityContext.TryParse(traceparent, tracestate, out var parentContext)
                ? WebApiFeederExtensions.ActivitySource.StartActivity("webapi receive", ActivityKind.Consumer, parentContext)
                : WebApiFeederExtensions.ActivitySource.StartActivity("webapi receive", ActivityKind.Consumer);
            activity?.SetTag("messaging.system", "webapi");
            activity?.SetTag("messaging.destination.name", FeederConfiguration.Path);
            activity?.SetTag("messaging.operation", "receive");

            var receiveTimestamp = Stopwatch.GetTimestamp();
            try
            {
                await ReceiveAsync(rawMessage, cancellationToken: cancellationToken).ConfigureAwait(false);
                ReportHealth(HealthStatus.Healthy);
                WebApiFeederExtensions.MessagesReceived.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                WebApiFeederExtensions.MessagesReceiveFailed.Add(1);
                ReportHealth(HealthStatus.Unhealthy, exception);
                Log.ProcessError(Logger, exception, FeederConfiguration.Path);
                throw;
            }
            finally
            {
                WebApiFeederExtensions.ReceiveDuration.Record(Stopwatch.GetElapsedTime(receiveTimestamp).TotalMilliseconds);
            }
        }
    }
}