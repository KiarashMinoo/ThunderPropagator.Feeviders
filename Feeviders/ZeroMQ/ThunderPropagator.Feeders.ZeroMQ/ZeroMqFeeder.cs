using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using NetMQ;
using NetMQ.Monitoring;
using NetMQ.Sockets;
using OpenTelemetry;
using ThunderPropagator.Application;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Features;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Feeders.ZeroMQ
{
    internal
#if !DEBUG
        sealed
#endif
        partial class ZeroMqFeeder<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration> : IterativeFeeder<TChannel, TZeroMqFeederMessage, TZeroMqFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TZeroMqFeederMessage : ZeroMqFeederMessage
        where TZeroMqFeederConfiguration : ZeroMqFeederConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4600, Level = LogLevel.Warning, Message = "{FeederName}/{ChannelName} is disabled (IsEnabled=false), skipping socket connection.")]
            public static partial void FeederDisabled(ILogger logger, string feederName, string channelName);

            [LoggerMessage(EventId = 4601, Level = LogLevel.Information, Message = "ZeroMQ feeder on Endpoint {Endpoint} connected.")]
            public static partial void Connected(ILogger logger, string endpoint);

            [LoggerMessage(EventId = 4602, Level = LogLevel.Warning, Message = "ZeroMQ feeder on Endpoint {Endpoint} disconnected.")]
            public static partial void Disconnected(ILogger logger, string endpoint);
        }

        private readonly NetMQSocket _socket;
        private readonly NetMQMonitor _monitor;
        private readonly Task _monitorTask;
        private readonly FormatDeserializerInvoker _formatDeserializerInvoker;

        public ZeroMqFeeder(TChannel channel,
            TZeroMqFeederConfiguration feederConfiguration,
            IFeederHandler<TChannel, TZeroMqFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, feederConfiguration, feederHandler, serviceProvider)
        {
            _socket = ZeroMqSocketFactory.CreateFeederSocket(feederConfiguration);

            if (feederConfiguration.SocketPattern == ZeroMqSocketPattern.PubSub && _socket is SubscriberSocket subscriberSocket)
                subscriberSocket.Subscribe(feederConfiguration.Topic ?? string.Empty);

            _monitor = new NetMQMonitor(_socket, $"inproc://zeromq-feeder-{Guid.NewGuid():N}", SocketEvents.Connected | SocketEvents.Disconnected);
            _monitor.Connected += OnConnected;
            _monitor.Disconnected += OnDisconnected;
            _monitorTask = _monitor.StartAsync();

            ZeroMqSocketFactory.ApplyOptionsAndConnect(_socket, feederConfiguration);

            _formatDeserializerInvoker = serviceProvider.GetRequiredService<FormatDeserializerInvoker>();
        }

        private void OnConnected(object? sender, NetMQMonitorSocketEventArgs eventArgs)
        {
            Log.Connected(Logger, FeederConfiguration.Endpoint);
            ReportHealth(HealthStatus.Healthy);
        }

        private void OnDisconnected(object? sender, NetMQMonitorSocketEventArgs eventArgs)
        {
            Log.Disconnected(Logger, FeederConfiguration.Endpoint);
            ReportHealth(HealthStatus.Unhealthy);
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

        protected override async IAsyncEnumerable<FeederReceivedMessage<TZeroMqFeederMessage>> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = new())
        {
            if (!FeederConfiguration.IsEnabled)
            {
                await Task.Yield();
                yield break;
            }

            var expectedFrameCount = FeederConfiguration.SocketPattern == ZeroMqSocketPattern.PubSub ? 4 : 3;

            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await _socket.ReceiveMultipartMessageAsync(expectedFrameCount, cancellationToken).ConfigureAwait(false);

                var stopwatch = Stopwatch.StartNew();
                var (feederMessage, activityContext, baggage) = ZeroMqEnvelopeTranslator.FromMessage<TZeroMqFeederMessage>(
                    FeederConfiguration.SocketPattern, message, _formatDeserializerInvoker, FeederConfiguration.SerializerType);

                using var activity = activityContext.HasValue
                    ? ZeroMqFeederExtensions.ActivitySource.StartActivity("zeromq receive", ActivityKind.Consumer, activityContext.Value)
                    : ZeroMqFeederExtensions.ActivitySource.StartActivity("zeromq receive", ActivityKind.Consumer);
                activity?.SetTag("messaging.system", "zeromq");
                activity?.SetTag("messaging.destination.name", FeederConfiguration.Endpoint);
                activity?.SetTag("messaging.operation", "receive");

                var settled = false;
                try
                {
                    yield return new FeederReceivedMessage<TZeroMqFeederMessage>(feederMessage, activityContext, baggage);
                    settled = true;
                }
                finally
                {
                    stopwatch.Stop();
                    if (settled)
                    {
                        ZeroMqFeederExtensions.MessagesReceived.Add(1);
                    }
                    else
                    {
                        ZeroMqFeederExtensions.MessagesReceiveFailed.Add(1);
                        activity?.SetStatus(ActivityStatusCode.Error);
                    }
                    ZeroMqFeederExtensions.ReceiveDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
                }
            }
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            _monitor.Connected -= OnConnected;
            _monitor.Disconnected -= OnDisconnected;
            _monitor.Stop();

            try
            {
                await _monitorTask.ConfigureAwait(false);
            }
            catch
            {
                // The monitor task faults once its underlying socket is torn down mid-poll; the shutdown itself already succeeded.
            }

            _monitor.Dispose();
            _socket.Dispose();
        }
    }
}
