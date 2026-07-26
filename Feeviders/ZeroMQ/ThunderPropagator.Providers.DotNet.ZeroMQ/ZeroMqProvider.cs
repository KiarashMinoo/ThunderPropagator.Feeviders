using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NetMQ;
using NetMQ.Monitoring;
using OpenTelemetry;
using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.ZeroMQ
{
    internal
#if !DEBUG
        sealed
#endif
        partial class ZeroMqProvider<TZeroMqProviderMessage, TZeroMqProviderConfiguration> : AbstractProvider<TZeroMqProviderMessage, TZeroMqProviderConfiguration>
        where TZeroMqProviderMessage : ZeroMqProviderMessage
        where TZeroMqProviderConfiguration : ZeroMqProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 4603, Level = LogLevel.Error, Message = "error has occured while producing message to Endpoint {Endpoint}.")]
            public static partial void ProduceException(ILogger logger, Exception exception, string endpoint);

            [LoggerMessage(EventId = 4604, Level = LogLevel.Information, Message = "ZeroMQ provider on Endpoint {Endpoint} connected.")]
            public static partial void Connected(ILogger logger, string endpoint);

            [LoggerMessage(EventId = 4605, Level = LogLevel.Warning, Message = "ZeroMQ provider on Endpoint {Endpoint} disconnected.")]
            public static partial void Disconnected(ILogger logger, string endpoint);
        }

        private readonly TZeroMqProviderConfiguration _zeroMqProviderConfiguration;
        private readonly NetMQSocket? _socket;
        private readonly NetMQMonitor? _monitor;
        private readonly Task? _monitorTask;
        private readonly IOutgoingSocket _outgoingSocket;

        public ZeroMqProvider(TZeroMqProviderConfiguration zeroMqProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _zeroMqProviderConfiguration = zeroMqProviderConfiguration;
            _socket = ZeroMqSocketFactory.CreateProviderSocket(zeroMqProviderConfiguration);

            _monitor = new NetMQMonitor(_socket, $"inproc://zeromq-provider-{Guid.NewGuid():N}", SocketEvents.Connected | SocketEvents.Disconnected);
            _monitor.Connected += OnConnected;
            _monitor.Disconnected += OnDisconnected;
            _monitorTask = _monitor.StartAsync();

            ZeroMqSocketFactory.ApplyOptionsAndConnect(_socket, zeroMqProviderConfiguration);

            _outgoingSocket = _socket;
        }

        // Test-only seam: no NetMQSocket/NetMQMonitor is created here, so DisposeManagedResourcesAsync has
        // nothing of its own to tear down for this path - see the null-guards there.
        internal ZeroMqProvider(TZeroMqProviderConfiguration zeroMqProviderConfiguration,
            IServiceProvider serviceProvider,
            IOutgoingSocket outgoingSocket)
            : base(serviceProvider)
        {
            _zeroMqProviderConfiguration = zeroMqProviderConfiguration;
            _outgoingSocket = outgoingSocket;
        }

        private void OnConnected(object? sender, NetMQMonitorSocketEventArgs eventArgs) => Log.Connected(Logger, _zeroMqProviderConfiguration.Endpoint);

        private void OnDisconnected(object? sender, NetMQMonitorSocketEventArgs eventArgs) => Log.Disconnected(Logger, _zeroMqProviderConfiguration.Endpoint);

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            using var activity = ZeroMqProviderExtensions.ActivitySource.StartActivity("zeromq publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "zeromq");
            activity?.SetTag("messaging.destination.name", _zeroMqProviderConfiguration.Endpoint);
            activity?.SetTag("messaging.operation", "publish");

            var stopwatch = Stopwatch.StartNew();

            try
            {
                var message = ZeroMqEnvelopeTranslator.ToMessage(_zeroMqProviderConfiguration.SocketPattern, _zeroMqProviderConfiguration.Topic, bytes);

                _outgoingSocket.SendMultipartMessage(message);

                ZeroMqProviderExtensions.MessagesPublished.Add(1);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                ZeroMqProviderExtensions.MessagesPublishFailed.Add(1);

                Log.ProduceException(Logger, exception, _zeroMqProviderConfiguration.Endpoint);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                ZeroMqProviderExtensions.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }

            return Task.CompletedTask;
        }

        protected override async ValueTask DisposeManagedResourcesAsync()
        {
            if (_monitor is not null)
            {
                _monitor.Connected -= OnConnected;
                _monitor.Disconnected -= OnDisconnected;
                _monitor.Stop();

                if (_monitorTask is not null)
                {
                    try
                    {
                        await _monitorTask.ConfigureAwait(false);
                    }
                    catch
                    {
                        // The monitor task faults once its underlying socket is torn down mid-poll; the shutdown itself already succeeded.
                    }
                }

                _monitor.Dispose();
            }

            _socket?.Dispose();
        }
    }
}
